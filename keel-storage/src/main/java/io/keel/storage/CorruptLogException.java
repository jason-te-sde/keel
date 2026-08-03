package io.keel.storage;

import java.nio.file.Path;

/**
 * Thrown when the log holds damage that cannot be explained by a crash.
 *
 * <p>A partial record at the very end of the newest segment is normal: a process can die between
 * the write and the sync, and that suffix was never acknowledged to anyone. Anything else is not
 * normal. A record that fails its checksum with valid records after it means the bytes were
 * corrupted after they were written, and everything past that point is suspect.
 *
 * <p>Recovery deliberately fails here instead of skipping ahead. Silently dropping a damaged record
 * would silently drop entries a quorum may have committed, and the cluster would carry on as though
 * nothing were wrong.
 */
public final class CorruptLogException extends RuntimeException {

    public CorruptLogException(Path file, long offset, String detail) {
        super("corrupt log at " + file.getFileName() + " offset " + offset + ": " + detail);
    }
}
