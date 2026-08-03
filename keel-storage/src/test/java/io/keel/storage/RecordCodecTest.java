package io.keel.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.keel.proto.log.Entry;
import io.keel.proto.log.EntryType;
import io.keel.proto.log.HardState;
import io.keel.proto.log.Record;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Record framing: what a reader can conclude from bytes a crash left behind. */
class RecordCodecTest {

    @Test
    @DisplayName("an entry round-trips")
    void entryRoundTrip() {
        Record record = Record.newBuilder().setEntry(entry(7, 3, "payload")).build();

        Record decoded = RecordCodec.decode(RecordCodec.encode(record));

        assertEquals(record, decoded);
    }

    @Test
    @DisplayName("hard state round-trips")
    void stateRoundTrip() {
        Record record =
                Record.newBuilder()
                        .setState(HardState.newBuilder().setTerm(9).setVote(3).setCommit(41))
                        .build();

        assertEquals(record, RecordCodec.decode(RecordCodec.encode(record)));
    }

    @Test
    @DisplayName("records read back one after another")
    void sequentialReads() {
        byte[] first = RecordCodec.encode(Record.newBuilder().setEntry(entry(1, 1, "a")).build());
        byte[] second = RecordCodec.encode(Record.newBuilder().setEntry(entry(2, 1, "bb")).build());
        ByteBuffer buf = ByteBuffer.allocate(first.length + second.length).put(first).put(second).flip();

        RecordCodec.ReadResult.Ok one =
                assertInstanceOf(RecordCodec.ReadResult.Ok.class, RecordCodec.read(buf, 1 << 20));
        RecordCodec.ReadResult.Ok two =
                assertInstanceOf(RecordCodec.ReadResult.Ok.class, RecordCodec.read(buf, 1 << 20));

        assertEquals(1, one.record().getEntry().getIndex());
        assertEquals(2, two.record().getEntry().getIndex());
        assertEquals(first.length, one.framedBytes());
        assertEquals(0, buf.remaining());
    }

    @Test
    @DisplayName("a flipped bit in the payload is caught by the checksum")
    void flippedBitIsCaught() {
        byte[] framed = RecordCodec.encode(Record.newBuilder().setEntry(entry(1, 1, "hello")).build());
        framed[framed.length - 1] ^= 0x40;

        RecordCodec.ReadResult result = RecordCodec.read(ByteBuffer.wrap(framed), 1 << 20);

        RecordCodec.ReadResult.Damaged damaged =
                assertInstanceOf(RecordCodec.ReadResult.Damaged.class, result);
        assertTrue(damaged.detail().contains("checksum"), damaged.detail());
        assertEquals(framed.length, damaged.framedBytes());
    }

    @Test
    @DisplayName("a record cut short is reported as unfinished, not as damage")
    void shortReadIsTruncation() {
        byte[] framed = RecordCodec.encode(Record.newBuilder().setEntry(entry(1, 1, "hello")).build());
        byte[] cut = new byte[framed.length - 3];
        System.arraycopy(framed, 0, cut, 0, cut.length);

        RecordCodec.ReadResult result = RecordCodec.read(ByteBuffer.wrap(cut), 1 << 20);

        // The distinction matters: unfinished is what a crash produces and is forgiven at the tail,
        // damage is not.
        assertInstanceOf(RecordCodec.ReadResult.Truncated.class, result);
    }

    @Test
    @DisplayName("a header too short to parse is unfinished")
    void partialHeaderIsTruncation() {
        assertInstanceOf(
                RecordCodec.ReadResult.Truncated.class,
                RecordCodec.read(ByteBuffer.wrap(new byte[] {0, 0, 1}), 1 << 20));
    }

    @Test
    @DisplayName("an implausible length is refused instead of allocated for")
    void implausibleLengthIsRefused() {
        // A crash can leave anything where a length prefix belongs. Trusting it here is how a
        // four-byte accident becomes a two-gigabyte allocation.
        ByteBuffer buf = ByteBuffer.allocate(64);
        buf.putInt(Integer.MAX_VALUE).putInt(0).flip();

        RecordCodec.ReadResult result = RecordCodec.read(buf, 1 << 20);

        RecordCodec.ReadResult.Truncated truncated =
                assertInstanceOf(RecordCodec.ReadResult.Truncated.class, result);
        assertTrue(truncated.detail().contains("implausible"), truncated.detail());
        assertEquals(0, buf.position(), "the buffer position should be left where it was");
    }

    @Test
    @DisplayName("a negative length is refused")
    void negativeLengthIsRefused() {
        ByteBuffer buf = ByteBuffer.allocate(64);
        buf.putInt(-17).putInt(0).flip();

        assertInstanceOf(RecordCodec.ReadResult.Truncated.class, RecordCodec.read(buf, 1 << 20));
    }

    @Test
    @DisplayName("a failed read leaves the buffer position untouched")
    void failedReadDoesNotConsume() {
        byte[] framed = RecordCodec.encode(Record.newBuilder().setEntry(entry(1, 1, "x")).build());
        framed[framed.length - 1] ^= 0x01;
        ByteBuffer buf = ByteBuffer.wrap(framed);
        buf.position(0);

        RecordCodec.read(buf, 1 << 20);

        // Recovery relies on this to report the offset the damage starts at.
        assertEquals(0, buf.position());
    }

    private static Entry entry(long index, long term, String data) {
        return Entry.newBuilder()
                .setIndex(index)
                .setTerm(term)
                .setType(EntryType.ENTRY_TYPE_NORMAL)
                .setData(ByteString.copyFrom(data, StandardCharsets.UTF_8))
                .build();
    }
}
