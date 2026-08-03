package io.keel.storage;

import com.google.protobuf.InvalidProtocolBufferException;
import io.keel.proto.log.Record;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32C;

/**
 * Framing for one log record: {@code [length int32][crc32c int32][payload]}.
 *
 * <p>The length and the checksum are what make a crash recoverable. Length alone would let a torn
 * write be mistaken for a valid record whose bytes happen to parse; a checksum alone would leave no
 * way to find where the next record starts. Both are big-endian so a hex dump of a segment is
 * readable.
 *
 * <p>CRC32C rather than CRC32 because it has a hardware instruction on every CPU this will run on,
 * and it is in the JDK, so it costs no dependency.
 */
final class RecordCodec {

    /** Bytes before the payload: a four-byte length and a four-byte checksum. */
    static final int HEADER_BYTES = 8;

    private RecordCodec() {}

    /** Frames a record for appending. */
    static byte[] encode(Record record) {
        byte[] payload = record.toByteArray();
        ByteBuffer buf = ByteBuffer.allocate(HEADER_BYTES + payload.length).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(payload.length);
        buf.putInt(checksum(payload, 0, payload.length));
        buf.put(payload);
        return buf.array();
    }

    /** Outcome of trying to read a record from a buffer. */
    sealed interface ReadResult {

        /** A complete, checksum-verified record. */
        record Ok(Record record, int framedBytes) implements ReadResult {}

        /**
         * The buffer ends before the record does, so this is a write that never finished. Expected
         * at the tail of the newest segment after a crash.
         */
        record Truncated(String detail) implements ReadResult {}

        /**
         * The record is complete but its bytes are wrong.
         *
         * @param framedBytes how far the record claimed to extend, so the caller can tell whether
         *     anything follows it. Damage with valid records after it cannot be explained by a crash.
         */
        record Damaged(String detail, int framedBytes) implements ReadResult {}
    }

    /**
     * Reads one record starting at {@code buf}'s position, leaving the position after it on success.
     *
     * @param maxRecordBytes an implausible length is treated as truncation rather than trusted, since
     *     a crash can leave anything at all where a length prefix should be
     */
    static ReadResult read(ByteBuffer buf, int maxRecordBytes) {
        if (buf.remaining() < HEADER_BYTES) {
            return new ReadResult.Truncated(
                    "only " + buf.remaining() + " bytes left, need " + HEADER_BYTES + " for a header");
        }
        int start = buf.position();
        int length = buf.getInt();
        int expectedCrc = buf.getInt();

        if (length < 0 || length > maxRecordBytes) {
            buf.position(start);
            return new ReadResult.Truncated("implausible record length " + length);
        }
        if (buf.remaining() < length) {
            buf.position(start);
            return new ReadResult.Truncated(
                    "record claims " + length + " bytes, only " + buf.remaining() + " remain");
        }

        byte[] payload = new byte[length];
        buf.get(payload);
        int actualCrc = checksum(payload, 0, length);
        if (actualCrc != expectedCrc) {
            buf.position(start);
            return new ReadResult.Damaged(
                    "checksum mismatch: stored "
                            + Integer.toHexString(expectedCrc)
                            + ", computed "
                            + Integer.toHexString(actualCrc),
                    HEADER_BYTES + length);
        }
        try {
            return new ReadResult.Ok(Record.parseFrom(payload), HEADER_BYTES + length);
        } catch (InvalidProtocolBufferException e) {
            // The checksum matched, so the bytes on disk are the bytes that were written. This is a
            // schema problem rather than a storage problem, and it must not be mistaken for one.
            buf.position(start);
            return new ReadResult.Damaged(
                    "checksum is valid but the payload does not parse: " + e.getMessage(),
                    HEADER_BYTES + length);
        }
    }

    /** Decodes a single framed record read from a known offset and length. */
    static Record decode(byte[] framed) {
        ByteBuffer buf = ByteBuffer.wrap(framed).order(ByteOrder.BIG_ENDIAN);
        ReadResult result = read(buf, framed.length);
        if (result instanceof ReadResult.Ok ok) {
            return ok.record();
        }
        throw new IllegalStateException("record at a known-good offset failed to decode: " + result);
    }

    private static int checksum(byte[] data, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(data, offset, length);
        return (int) crc.getValue();
    }
}
