package io.keel.node;

import io.keel.proto.log.SnapshotMetadata;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.zip.CRC32C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * State machine snapshots on disk.
 *
 * <p>A snapshot is only ever promoted once it is complete: written to a temporary file, fsynced,
 * checksummed, and then renamed into place. Writing directly to the final name would leave a
 * half-written file that recovery cannot distinguish from a good one, and the log has already been
 * compacted on the strength of it.
 *
 * <p>Older snapshots are kept for a while rather than deleted immediately. They cost disk and they buy
 * a fallback: a snapshot whose checksum fails is a bug worth surviving, not worth crashing on.
 */
final class SnapshotStore {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotStore.class);
    private static final String SUFFIX = ".snap";
    private static final int KEEP = 3;

    /** A snapshot on disk: its boundary and where the bytes are. */
    record Stored(SnapshotMetadata meta, Path file) {}

    private final Path directory;

    SnapshotStore(Path directory) {
        this.directory = directory;
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to create " + directory, e);
        }
    }

    /**
     * Writes a snapshot and returns its metadata with the size and checksum filled in.
     *
     * <p>The returned metadata is what should be handed to {@code LogStore.compact}: it describes what
     * is actually on disk, not what the caller intended to write.
     */
    SnapshotMetadata write(SnapshotMetadata meta, byte[] payload) {
        SnapshotMetadata complete =
                meta.toBuilder().setSizeBytes(payload.length).setChecksum(checksum(payload)).build();
        Path temp = directory.resolve(fileName(complete) + ".tmp");
        Path target = directory.resolve(fileName(complete));

        try (OutputStream out =
                Files.newOutputStream(
                        temp, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            complete.writeDelimitedTo(out);
            out.write(payload);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to write a snapshot to " + temp, e);
        }
        syncFile(temp);
        try {
            Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to promote " + temp, e);
        }
        syncDirectory();
        LOG.info(
                "wrote a snapshot at index {} ({} bytes)", complete.getLastIndex(), payload.length);
        prune();
        return complete;
    }

    /** The newest snapshot on disk, or empty when there is none. */
    Optional<Stored> latest() {
        List<Stored> all = list();
        return all.isEmpty() ? Optional.empty() : Optional.of(all.get(all.size() - 1));
    }

    /**
     * Opens a snapshot's payload for streaming, positioned after the metadata header.
     *
     * <p>The checksum cannot be verified up front without reading the whole payload, which is the
     * thing this method exists to avoid. Verification is the sender's job over the wire and the
     * receiver's job on arrival: the receiver has to checksum what it actually received anyway, so
     * checking here as well would read every byte twice to catch nothing new.
     */
    InputStream open(Stored stored) {
        try {
            InputStream in = Files.newInputStream(stored.file());
            // Consume the delimited header so the caller sees payload from its first byte.
            SnapshotMetadata header = SnapshotMetadata.parseDelimitedFrom(in);
            if (header == null) {
                in.close();
                throw new IllegalStateException("snapshot " + stored.file() + " has no metadata");
            }
            return in;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to open " + stored.file(), e);
        }
    }

    /**
     * Reads a snapshot's payload whole, verifying its checksum.
     *
     * <p>Used on startup, where the state machine is being restored and the payload has to be in
     * memory regardless. Transfers use {@link #open} instead.
     *
     * @throws IllegalStateException if the bytes do not match the metadata, which means the file was
     *     damaged after it was written
     */
    byte[] read(Stored stored) {
        try (InputStream in = Files.newInputStream(stored.file())) {
            SnapshotMetadata meta = SnapshotMetadata.parseDelimitedFrom(in);
            if (meta == null) {
                throw new IllegalStateException("snapshot " + stored.file() + " has no metadata");
            }
            byte[] payload = in.readAllBytes();
            if (payload.length != meta.getSizeBytes()) {
                throw new IllegalStateException(
                        "snapshot "
                                + stored.file().getFileName()
                                + " claims "
                                + meta.getSizeBytes()
                                + " bytes but holds "
                                + payload.length);
            }
            if (checksum(payload) != meta.getChecksum()) {
                throw new IllegalStateException(
                        "snapshot " + stored.file().getFileName() + " failed its checksum");
            }
            return payload;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read " + stored.file(), e);
        }
    }

    /**
     * Starts receiving a snapshot, written straight to disk as chunks arrive.
     *
     * <p>Nothing larger than one chunk is ever in memory. Buffering a whole snapshot capped the
     * store's practical size at the heap, for a reason that had nothing to do with consensus.
     */
    Incoming receive(SnapshotMetadata promised) {
        return new Incoming(promised);
    }

    /**
     * A snapshot being written as it arrives.
     *
     * <p>Size and checksum are only known once everything has arrived, and the header has to sit in
     * front of the payload, so the body lands in a scratch file and the header is prepended at the
     * end. Two files on disk rather than one array in memory is the whole point.
     */
    final class Incoming implements AutoCloseable {

        private final SnapshotMetadata promised;
        private final Path scratch;
        private final CRC32C crc = new CRC32C();
        private final OutputStream body;
        private long size;
        private boolean finished;

        private Incoming(SnapshotMetadata promised) {
            this.promised = promised;
            this.scratch = directory.resolve(fileName(promised) + ".body");
            try {
                this.body =
                        Files.newOutputStream(
                                scratch, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            } catch (IOException e) {
                throw new UncheckedIOException("failed to open " + scratch, e);
            }
        }

        void write(byte[] chunk, int length) {
            try {
                crc.update(chunk, 0, length);
                size += length;
                body.write(chunk, 0, length);
            } catch (IOException e) {
                throw new UncheckedIOException("failed to write a snapshot chunk", e);
            }
        }

        /**
         * Verifies what arrived against what was promised, then promotes the file.
         *
         * @throws IllegalStateException if the size or checksum disagree, so a truncated or corrupted
         *     transfer is rejected rather than installed
         */
        SnapshotMetadata finish() {
            Path temp = directory.resolve(fileName(promised) + ".tmp");
            try {
                body.close();
                if (promised.getSizeBytes() != 0 && size != promised.getSizeBytes()) {
                    throw new IllegalStateException(
                            "received "
                                    + size
                                    + " bytes for a snapshot that promised "
                                    + promised.getSizeBytes());
                }
                if (promised.getChecksum() != 0 && crc.getValue() != promised.getChecksum()) {
                    throw new IllegalStateException("received snapshot failed its checksum");
                }
                SnapshotMetadata complete =
                        promised.toBuilder().setSizeBytes(size).setChecksum(crc.getValue()).build();
                try (OutputStream out =
                                Files.newOutputStream(
                                        temp,
                                        StandardOpenOption.CREATE,
                                        StandardOpenOption.TRUNCATE_EXISTING);
                        InputStream in = Files.newInputStream(scratch)) {
                    complete.writeDelimitedTo(out);
                    in.transferTo(out);
                }
                syncAndPromote(temp, complete);
                finished = true;
                return complete;
            } catch (IOException e) {
                throw new UncheckedIOException("failed to store a received snapshot", e);
            } finally {
                deleteQuietly(scratch);
                if (!finished) {
                    deleteQuietly(temp);
                }
            }
        }

        @Override
        public void close() {
            if (finished) {
                return;
            }
            // An abandoned transfer leaves nothing behind. A half-written snapshot that survived would
            // be indistinguishable from a good one, and the log gets compacted on the strength of it.
            try {
                body.close();
            } catch (IOException e) {
                LOG.debug("could not close {}: {}", scratch, e.getMessage());
            }
            deleteQuietly(scratch);
        }
    }

    private void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            // A leftover temporary file costs disk and nothing else; the next transfer truncates it.
            LOG.debug("could not delete {}: {}", path, e.getMessage());
        }
    }

    private void syncAndPromote(Path temp, SnapshotMetadata complete) {
        syncFile(temp);
        Path target = directory.resolve(fileName(complete));
        try {
            Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to promote " + temp, e);
        }
        syncDirectory();
        LOG.info(
                "stored a snapshot at index {} ({} bytes)",
                complete.getLastIndex(),
                complete.getSizeBytes());
        prune();
    }

    /** Stores a snapshot received from a leader, after checking it against the promised metadata. */
    SnapshotMetadata accept(SnapshotMetadata meta, byte[] payload) {
        if (meta.getSizeBytes() != 0 && payload.length != meta.getSizeBytes()) {
            throw new IllegalStateException(
                    "received "
                            + payload.length
                            + " bytes for a snapshot that promised "
                            + meta.getSizeBytes());
        }
        if (meta.getChecksum() != 0 && checksum(payload) != meta.getChecksum()) {
            throw new IllegalStateException("received snapshot failed its checksum");
        }
        return write(meta, payload);
    }

    private List<Stored> list() {
        List<Stored> found = new ArrayList<>();
        try (var files = Files.list(directory)) {
            for (Path path : files.toList()) {
                String name = path.getFileName().toString();
                if (!name.endsWith(SUFFIX)) {
                    continue;
                }
                try (InputStream in = Files.newInputStream(path)) {
                    SnapshotMetadata meta = SnapshotMetadata.parseDelimitedFrom(in);
                    if (meta != null) {
                        found.add(new Stored(meta, path));
                    }
                } catch (IOException e) {
                    LOG.warn("ignoring unreadable snapshot {}: {}", name, e.getMessage());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to list " + directory, e);
        }
        found.sort(Comparator.comparingLong(stored -> stored.meta().getLastIndex()));
        return found;
    }

    private void prune() {
        List<Stored> all = list();
        for (int i = 0; i < all.size() - KEEP; i++) {
            try {
                Files.deleteIfExists(all.get(i).file());
            } catch (IOException e) {
                LOG.warn("could not delete an old snapshot: {}", e.getMessage());
            }
        }
    }

    private static String fileName(SnapshotMetadata meta) {
        return String.format("%020d-%020d%s", meta.getLastIndex(), meta.getLastTerm(), SUFFIX);
    }

    private static long checksum(byte[] payload) {
        CRC32C crc = new CRC32C();
        crc.update(payload, 0, payload.length);
        return crc.getValue();
    }

    private static void syncFile(Path path) {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            channel.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to sync " + path, e);
        }
    }

    private void syncDirectory() {
        // Renaming a file is not durable until its directory is, and the log is about to be compacted
        // on the strength of this snapshot existing.
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException e) {
            LOG.warn("could not sync the snapshot directory: {}", e.getMessage());
        }
    }
}
