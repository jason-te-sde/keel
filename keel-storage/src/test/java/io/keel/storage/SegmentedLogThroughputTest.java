package io.keel.storage;

import io.keel.proto.log.Entry;
import io.keel.raft.Entries;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

/**
 * Prints append throughput for the numbers quoted in the documentation.
 *
 * <p>Off by default: it measures the machine as much as the code, so failing a build on it would make
 * CI a hardware monitor. Run it explicitly:
 *
 * <pre>
 *   mvn test -Dkeel.bench=true -Dtest=SegmentedLogThroughputTest \
 *       -Dsurefire.failIfNoSpecifiedTests=false
 * </pre>
 *
 * <p>The interesting number is the ratio rather than either absolute figure. Syncing per entry is
 * one fsync per write; syncing per batch is one fsync per batch, which is what the node layer
 * actually does when it drains a Ready. Anything that looks like millions of durable writes per
 * second is measuring the page cache, not the disk.
 */
class SegmentedLogThroughputTest {

    private static final int ENTRIES = 20_000;
    private static final int PAYLOAD_BYTES = 256;
    private static final int BATCH = 64;

    @TempDir Path dir;

    @Test
    @EnabledIfSystemProperty(named = "keel.bench", matches = "true")
    @DisplayName("append throughput, with and without a sync per write")
    void appendThroughput() {
        report("no sync", run(Mode.NO_SYNC));
        report("sync per batch of " + BATCH, run(Mode.SYNC_PER_BATCH));
        report("sync per entry", run(Mode.SYNC_PER_ENTRY));
    }

    private enum Mode {
        NO_SYNC,
        SYNC_PER_BATCH,
        SYNC_PER_ENTRY
    }

    private double run(Mode mode) {
        Path target = dir.resolve(mode.name().toLowerCase(Locale.ROOT));
        byte[] payload = new byte[PAYLOAD_BYTES];
        try (SegmentedLog log = SegmentedLog.open(LogOptions.of(target))) {
            long start = System.nanoTime();
            for (int i = 0; i < ENTRIES; i += BATCH) {
                List<Entry> batch = new ArrayList<>(BATCH);
                for (int j = 0; j < BATCH && i + j < ENTRIES; j++) {
                    batch.add(Entries.normal(i + j + 1L, 1, payload));
                }
                if (mode == Mode.SYNC_PER_ENTRY) {
                    for (Entry e : batch) {
                        log.append(List.of(e));
                        log.sync();
                    }
                } else {
                    log.append(batch);
                    if (mode == Mode.SYNC_PER_BATCH) {
                        log.sync();
                    }
                }
            }
            if (mode == Mode.NO_SYNC) {
                log.sync();
            }
            double seconds = (System.nanoTime() - start) / 1e9;
            return ENTRIES / seconds;
        }
    }

    private static void report(String label, double entriesPerSecond) {
        System.out.printf(
                Locale.ROOT,
                "%-28s %,12.0f entries/s  %6.1f MiB/s%n",
                label,
                entriesPerSecond,
                entriesPerSecond * PAYLOAD_BYTES / (1024 * 1024));
    }
}
