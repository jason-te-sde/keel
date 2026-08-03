package io.keel.storage;

import java.nio.file.Path;

/**
 * Tuning for {@link SegmentedLog}.
 *
 * @param directory where segment files live; created if missing
 * @param maxSegmentBytes soft cap on a segment file. A segment is rotated once it would exceed this,
 *     so a single record larger than the cap still gets written rather than rejected. Smaller
 *     segments reclaim space sooner after a snapshot and make recovery more incremental; larger ones
 *     mean fewer files and fewer directory syncs.
 * @param maxRecordBytes hard cap on one record, used during recovery to reject an absurd length
 *     before allocating for it. A crash can leave arbitrary bytes where a length prefix should be,
 *     and without this bound a corrupt four-byte length becomes a two-gigabyte allocation.
 */
public record LogOptions(Path directory, long maxSegmentBytes, int maxRecordBytes) {

    private static final long DEFAULT_SEGMENT_BYTES = 64L << 20;
    private static final int DEFAULT_MAX_RECORD_BYTES = 32 << 20;

    public LogOptions {
        if (directory == null) {
            throw new IllegalArgumentException("directory is required");
        }
        if (maxSegmentBytes <= 0) {
            throw new IllegalArgumentException("maxSegmentBytes must be positive");
        }
        if (maxRecordBytes <= 0) {
            throw new IllegalArgumentException("maxRecordBytes must be positive");
        }
    }

    public static LogOptions of(Path directory) {
        return new LogOptions(directory, DEFAULT_SEGMENT_BYTES, DEFAULT_MAX_RECORD_BYTES);
    }

    public LogOptions withMaxSegmentBytes(long bytes) {
        return new LogOptions(directory, bytes, maxRecordBytes);
    }

    public LogOptions withMaxRecordBytes(int bytes) {
        return new LogOptions(directory, maxSegmentBytes, bytes);
    }
}
