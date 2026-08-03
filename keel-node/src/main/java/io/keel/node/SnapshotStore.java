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
     * Reads a snapshot's payload, verifying its checksum.
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
