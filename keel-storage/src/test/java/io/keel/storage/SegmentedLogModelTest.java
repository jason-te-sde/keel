package io.keel.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.keel.proto.log.Entry;
import io.keel.proto.log.HardState;
import io.keel.raft.Entries;
import io.keel.raft.MemoryLogStore;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Differential test: the on-disk log has to behave exactly like the in-memory one.
 *
 * <p>{@link MemoryLogStore} is a few dozen lines and obviously correct by inspection, which makes it
 * a usable oracle. {@link SegmentedLog} has segment rotation, framing, checksums, and a replay that
 * has to reconstruct the same view from records written in a different shape. Comparing the two
 * across random operation sequences covers combinations nobody would think to write by hand, in
 * particular a superseding append that lands mid-segment and is then recovered.
 *
 * <p>Each case is a fixed seed, so a failure names the sequence that produced it.
 */
class SegmentedLogModelTest {

    private static final int OPERATIONS = 300;

    @TempDir Path root;

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233})
    @DisplayName("random append, overwrite, and reopen sequences match the in-memory store")
    void matchesTheInMemoryModel(long seed) {
        Path dir = root.resolve("seed-" + seed);
        Random random = new Random(seed);
        MemoryLogStore model = new MemoryLogStore();
        // A small segment cap so a 300-operation run crosses rotation boundaries many times.
        LogOptions options = LogOptions.of(dir).withMaxSegmentBytes(512);
        SegmentedLog log = SegmentedLog.open(options);

        try {
            for (int op = 0; op < OPERATIONS; op++) {
                String action =
                        switch (random.nextInt(10)) {
                            case 0, 1, 2, 3, 4 -> appendAtEnd(random, model, log);
                            case 5, 6 -> overwriteSuffix(random, model, log);
                            case 7 -> saveState(random, model, log);
                            case 8 -> sync(model, log);
                            default -> "reopen";
                        };

                if (action.equals("reopen")) {
                    // Only synced data is promised to survive, so sync before comparing across a
                    // reopen. What is under test here is replay, not durability.
                    model.sync();
                    log.sync();
                    log.close();
                    log = SegmentedLog.open(options);
                }
                assertSameView(model, log, seed, op, action);
            }
        } finally {
            log.close();
        }
    }

    private static String appendAtEnd(Random random, MemoryLogStore model, SegmentedLog log) {
        long start = model.lastIndex() + 1;
        List<Entry> batch = batch(random, start, 1 + random.nextInt(4));
        model.append(batch);
        log.append(batch);
        return "append " + batch.size() + " at " + start;
    }

    private static String overwriteSuffix(Random random, MemoryLogStore model, SegmentedLog log) {
        if (model.lastIndex() < 1) {
            return appendAtEnd(random, model, log);
        }
        long at = 1 + random.nextLong(model.lastIndex());
        List<Entry> batch = batch(random, at, 1 + random.nextInt(3));
        model.append(batch);
        log.append(batch);
        return "overwrite from " + at + " with " + batch.size();
    }

    private static String saveState(Random random, MemoryLogStore model, SegmentedLog log) {
        HardState state =
                HardState.newBuilder()
                        .setTerm(random.nextInt(20))
                        .setVote(random.nextInt(5))
                        .setCommit(Math.max(0, model.lastIndex() - random.nextInt(3)))
                        .build();
        model.saveHardState(state);
        log.saveHardState(state);
        return "saveHardState term=" + state.getTerm();
    }

    private static String sync(MemoryLogStore model, SegmentedLog log) {
        model.sync();
        log.sync();
        return "sync";
    }

    private static List<Entry> batch(Random random, long start, int count) {
        List<Entry> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long index = start + i;
            byte[] data = ("v" + index + "-" + random.nextInt(1000)).getBytes(StandardCharsets.UTF_8);
            out.add(Entries.normal(index, 1 + random.nextInt(9), data));
        }
        return out;
    }

    private static void assertSameView(
            MemoryLogStore model, SegmentedLog log, long seed, int op, String action) {
        String where = " (seed " + seed + ", operation " + op + ": " + action + ")";
        assertEquals(model.firstIndex(), log.firstIndex(), "firstIndex" + where);
        assertEquals(model.lastIndex(), log.lastIndex(), "lastIndex" + where);
        assertEquals(model.hardState(), log.hardState(), "hardState" + where);

        for (long i = model.firstIndex() - 1; i <= model.lastIndex(); i++) {
            assertEquals(model.term(i), log.term(i), "term at " + i + where);
        }
        if (model.lastIndex() >= model.firstIndex()) {
            List<Entry> expected = model.entries(model.firstIndex(), model.lastIndex() + 1, Long.MAX_VALUE);
            List<Entry> actual = log.entries(log.firstIndex(), log.lastIndex() + 1, Long.MAX_VALUE);
            assertEquals(expected, actual, "entries" + where);
        }
    }
}
