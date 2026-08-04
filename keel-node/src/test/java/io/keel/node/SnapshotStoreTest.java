package io.keel.node;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.keel.proto.log.SnapshotMetadata;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Snapshot files, including the streaming path.
 *
 * <p>The interesting cases are the ones that must not produce a usable file: a transfer that stops
 * halfway, and one whose bytes do not match what was promised. A half-written snapshot that survived
 * would be indistinguishable from a good one, and the log gets compacted on the strength of it.
 */
class SnapshotStoreTest {

    @TempDir Path dir;

    @Test
    @DisplayName("a snapshot round-trips through the streaming path")
    void streamingRoundTrip() throws IOException {
        SnapshotStore store = new SnapshotStore(dir);
        byte[] payload = random(700_000);
        SnapshotMetadata promised = boundary(42, 7);

        SnapshotMetadata stored;
        try (SnapshotStore.Incoming incoming = store.receive(promised)) {
            feed(incoming, payload, 64 * 1024);
            stored = incoming.finish();
        }

        assertEquals(payload.length, stored.getSizeBytes());
        assertEquals(42, stored.getLastIndex());
        assertEquals(7, stored.getLastTerm());
        // Larger than any single chunk, so this only passes if chunks were assembled in order.
        assertArrayEquals(payload, store.read(store.latest().orElseThrow()));
    }

    @Test
    @DisplayName("open() hands back the payload without the header")
    void openSkipsTheHeader() throws IOException {
        SnapshotStore store = new SnapshotStore(dir);
        byte[] payload = random(300_000);
        SnapshotMetadata written = store.write(boundary(10, 2), payload);

        byte[] read;
        try (InputStream in = store.open(store.latest().orElseThrow())) {
            read = in.readAllBytes();
        }

        assertArrayEquals(payload, read, "the metadata header must not leak into the payload");
        assertEquals(payload.length, written.getSizeBytes());
    }

    @Test
    @DisplayName("a truncated transfer is refused and leaves nothing behind")
    void truncatedTransferIsRefused() throws IOException {
        SnapshotStore store = new SnapshotStore(dir);
        byte[] payload = random(200_000);
        SnapshotMetadata full = store.write(boundary(5, 1), payload);
        // A second store directory, so the promise carries a size the transfer will not deliver.
        SnapshotStore target = new SnapshotStore(dir.resolve("target"));

        try (SnapshotStore.Incoming incoming = target.receive(full)) {
            feed(incoming, java.util.Arrays.copyOf(payload, payload.length / 2), 64 * 1024);
            IllegalStateException e = assertThrows(IllegalStateException.class, incoming::finish);
            assertTrue(e.getMessage().contains("promised"), e.getMessage());
        }

        assertTrue(target.latest().isEmpty(), "a refused transfer must not leave a usable snapshot");
        assertTrue(noTemporaryFilesIn(dir.resolve("target")), "and must not leave scratch files");
    }

    @Test
    @DisplayName("a corrupted transfer is refused")
    void corruptedTransferIsRefused() throws IOException {
        SnapshotStore store = new SnapshotStore(dir);
        byte[] payload = random(120_000);
        SnapshotMetadata full = store.write(boundary(5, 1), payload);
        SnapshotStore target = new SnapshotStore(dir.resolve("target"));

        byte[] damaged = payload.clone();
        damaged[damaged.length / 2] ^= 0x40;

        try (SnapshotStore.Incoming incoming = target.receive(full)) {
            feed(incoming, damaged, 64 * 1024);
            IllegalStateException e = assertThrows(IllegalStateException.class, incoming::finish);
            assertTrue(e.getMessage().contains("checksum"), e.getMessage());
        }

        assertTrue(target.latest().isEmpty());
    }

    @Test
    @DisplayName("an abandoned transfer leaves nothing behind")
    void abandonedTransferCleansUp() throws IOException {
        SnapshotStore store = new SnapshotStore(dir);

        try (SnapshotStore.Incoming incoming = store.receive(boundary(9, 3))) {
            feed(incoming, random(50_000), 16 * 1024);
            // No finish(): the sender died mid-transfer.
        }

        assertTrue(store.latest().isEmpty(), "nothing should be promoted");
        assertTrue(noTemporaryFilesIn(dir), "and no scratch file should survive");
    }

    @Test
    @DisplayName("an empty state machine is still a valid snapshot")
    void emptyPayload() {
        SnapshotStore store = new SnapshotStore(dir);

        SnapshotMetadata stored;
        try (SnapshotStore.Incoming incoming = store.receive(boundary(1, 1))) {
            stored = incoming.finish();
        }

        assertEquals(0, stored.getSizeBytes());
        assertEquals(0, store.read(store.latest().orElseThrow()).length);
    }

    @Test
    @DisplayName("only the newest few snapshots are kept")
    void oldSnapshotsArePruned() throws IOException {
        SnapshotStore store = new SnapshotStore(dir);
        for (int i = 1; i <= 6; i++) {
            store.write(boundary(i * 10, 1), random(1024));
        }

        List<Path> files;
        try (var paths = Files.list(dir)) {
            files = paths.filter(p -> p.toString().endsWith(".snap")).toList();
        }
        assertEquals(3, files.size(), "three are kept as a fallback if the newest is damaged");
        assertEquals(60, store.latest().orElseThrow().meta().getLastIndex());
    }

    @Test
    @DisplayName("a snapshot damaged on disk is refused on read")
    void damagedFileIsRefusedOnRead() throws IOException {
        SnapshotStore store = new SnapshotStore(dir);
        store.write(boundary(3, 1), random(4096));
        Path file = store.latest().orElseThrow().file();
        byte[] bytes = Files.readAllBytes(file);
        bytes[bytes.length - 1] ^= 0x20;
        Files.write(file, bytes);

        IllegalStateException e =
                assertThrows(IllegalStateException.class, () -> store.read(store.latest().orElseThrow()));
        assertTrue(e.getMessage().contains("checksum"), e.getMessage());
    }

    private static void feed(SnapshotStore.Incoming incoming, byte[] payload, int chunk)
            throws IOException {
        try (InputStream in = new ByteArrayInputStream(payload)) {
            byte[] buffer = new byte[chunk];
            int read;
            while ((read = in.read(buffer)) > 0) {
                incoming.write(buffer, read);
            }
        }
    }

    private static boolean noTemporaryFilesIn(Path directory) throws IOException {
        if (!Files.isDirectory(directory)) {
            return true;
        }
        try (var paths = Files.list(directory)) {
            return paths.noneMatch(p -> p.toString().endsWith(".tmp") || p.toString().endsWith(".body"));
        }
    }

    private static SnapshotMetadata boundary(long index, long term) {
        return SnapshotMetadata.newBuilder().setLastIndex(index).setLastTerm(term).build();
    }

    private static byte[] random(int size) {
        byte[] bytes = new byte[size];
        new Random(size).nextBytes(bytes);
        return bytes;
    }
}
