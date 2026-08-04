package io.keel.node;

import java.util.concurrent.atomic.LongAdder;

/**
 * Counters for one node.
 *
 * <p>Counters only ever go up, and gauges are read from live state at scrape time rather than being
 * mirrored here. Mirroring a gauge means two sources of truth for the same number, and the copy is
 * the one that goes stale.
 *
 * <p>{@link LongAdder} rather than {@code AtomicLong} because these are written from the raft thread,
 * the apply thread, and gRPC threads at once, and read once every fifteen seconds. That is exactly
 * the trade LongAdder is for.
 */
final class NodeMetrics {

    private final LongAdder writesAccepted = new LongAdder();
    private final LongAdder writesRejected = new LongAdder();
    private final LongAdder writesOverwritten = new LongAdder();
    private final LongAdder readsServed = new LongAdder();
    private final LongAdder readsFailed = new LongAdder();
    private final LongAdder entriesApplied = new LongAdder();
    private final LongAdder snapshotsTaken = new LongAdder();
    private final LongAdder snapshotsInstalled = new LongAdder();
    private final LongAdder snapshotsSent = new LongAdder();
    private final LongAdder snapshotSendFailures = new LongAdder();
    private final LongAdder membershipChanges = new LongAdder();
    private final LongAdder peerSendFailures = new LongAdder();

    void writeAccepted() {
        writesAccepted.increment();
    }

    /** A write refused before it entered the log, such as one offered to a follower. */
    void writeRejected() {
        writesRejected.increment();
    }

    /** A write whose index was taken by another command, so it never committed. */
    void writeOverwritten() {
        writesOverwritten.increment();
    }

    void readServed() {
        readsServed.increment();
    }

    void readFailed() {
        readsFailed.increment();
    }

    void entryApplied() {
        entriesApplied.increment();
    }

    void snapshotTaken() {
        snapshotsTaken.increment();
    }

    void snapshotInstalled() {
        snapshotsInstalled.increment();
    }

    void snapshotSent() {
        snapshotsSent.increment();
    }

    void snapshotSendFailed() {
        snapshotSendFailures.increment();
    }

    void membershipChanged() {
        membershipChanges.increment();
    }

    void peerSendFailed() {
        peerSendFailures.increment();
    }

    /** A consistent-enough view for one scrape. */
    Snapshot snapshot() {
        return new Snapshot(
                writesAccepted.sum(),
                writesRejected.sum(),
                writesOverwritten.sum(),
                readsServed.sum(),
                readsFailed.sum(),
                entriesApplied.sum(),
                snapshotsTaken.sum(),
                snapshotsInstalled.sum(),
                snapshotsSent.sum(),
                snapshotSendFailures.sum(),
                membershipChanges.sum(),
                peerSendFailures.sum());
    }

    record Snapshot(
            long writesAccepted,
            long writesRejected,
            long writesOverwritten,
            long readsServed,
            long readsFailed,
            long entriesApplied,
            long snapshotsTaken,
            long snapshotsInstalled,
            long snapshotsSent,
            long snapshotSendFailures,
            long membershipChanges,
            long peerSendFailures) {}
}
