package io.keel.storage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * One segment file.
 *
 * <p>Named {@code <sequence>-<baseIndex>.log}, both zero-padded. The sequence is what recovery orders
 * by, and it has to be: a superseding append can roll a new segment whose base index is lower than an
 * existing segment's, so base index alone does not give write order. Replaying out of write order
 * would let a superseded entry win over its replacement.
 *
 * <p>The base index stays in the name as a hint for locating a range without reading every file. It is
 * only ever a hint, since a superseding append can put lower indexes into a segment after it was
 * named.
 */
final class Segment implements AutoCloseable {

    private static final String SUFFIX = ".log";

    private final Path path;
    private final long sequence;
    private final long baseIndex;
    private final FileChannel channel;
    private long size;

    private Segment(Path path, long sequence, long baseIndex, FileChannel channel, long size) {
        this.path = path;
        this.sequence = sequence;
        this.baseIndex = baseIndex;
        this.channel = channel;
        this.size = size;
    }

    static String fileName(long sequence, long baseIndex) {
        return String.format("%020d-%020d%s", sequence, baseIndex, SUFFIX);
    }

    /** Order this segment was created in, or -1 if the name is not a segment name. */
    static long parseSequence(Path file) {
        return parseField(file, 0);
    }

    /** The base index encoded in a segment file name, or -1 if the name is not a segment name. */
    static long parseBaseIndex(Path file) {
        return parseField(file, 1);
    }

    private static long parseField(Path file, int field) {
        String name = file.getFileName().toString();
        if (!name.endsWith(SUFFIX)) {
            return -1;
        }
        String[] parts = name.substring(0, name.length() - SUFFIX.length()).split("-");
        if (parts.length != 2 || parts[0].length() != 20 || parts[1].length() != 20) {
            return -1;
        }
        try {
            return Long.parseLong(parts[field]);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    static Segment create(Path directory, long sequence, long baseIndex) throws IOException {
        Path path = directory.resolve(fileName(sequence, baseIndex));
        FileChannel channel =
                FileChannel.open(
                        path,
                        StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE);
        return new Segment(path, sequence, baseIndex, channel, 0);
    }

    static Segment open(Path path) throws IOException {
        FileChannel channel =
                FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        return new Segment(
                path, parseSequence(path), parseBaseIndex(path), channel, channel.size());
    }

    Path path() {
        return path;
    }

    long sequence() {
        return sequence;
    }

    long baseIndex() {
        return baseIndex;
    }

    long size() {
        return size;
    }

    /** Appends framed bytes at the end and returns the offset they were written at. */
    long append(byte[] framed) {
        try {
            long offset = size;
            ByteBuffer buf = ByteBuffer.wrap(framed);
            while (buf.hasRemaining()) {
                channel.write(buf, size + buf.position());
            }
            size += framed.length;
            return offset;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to append to " + path, e);
        }
    }

    /** Reads exactly {@code length} bytes at {@code offset}. */
    byte[] read(long offset, int length) {
        try {
            ByteBuffer buf = ByteBuffer.allocate(length);
            while (buf.hasRemaining()) {
                int read = channel.read(buf, offset + buf.position());
                if (read < 0) {
                    throw new IllegalStateException(
                            "segment " + path.getFileName() + " ended while reading " + length
                                    + " bytes at " + offset);
                }
            }
            return buf.array();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read from " + path, e);
        }
    }

    /** The whole file, for a sequential recovery scan. */
    ByteBuffer readAll() {
        try {
            ByteBuffer buf = ByteBuffer.allocate((int) Math.min(size, Integer.MAX_VALUE));
            while (buf.hasRemaining() && channel.read(buf, buf.position()) > 0) {
                // keep reading
            }
            return buf.flip();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read " + path, e);
        }
    }

    /**
     * Discards everything past {@code newSize}.
     *
     * <p>Used only by recovery, to remove a torn tail. Leaving the garbage in place would be worse
     * than losing it: the next append would land after it, and the following recovery would stop at
     * the garbage and silently lose everything written beyond it.
     */
    void truncateTo(long newSize) {
        try {
            channel.truncate(newSize);
            size = newSize;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to truncate " + path, e);
        }
    }

    void sync() {
        try {
            channel.force(false);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to sync " + path, e);
        }
    }

    void delete() {
        try {
            channel.close();
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to delete " + path, e);
        }
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to close " + path, e);
        }
    }

    @Override
    public String toString() {
        return "Segment["
                + path.getFileName()
                + " seq="
                + sequence
                + " base="
                + baseIndex
                + " size="
                + size
                + "]";
    }
}
