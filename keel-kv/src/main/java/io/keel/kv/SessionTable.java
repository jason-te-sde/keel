package io.keel.kv;

import io.keel.proto.kv.CommandResult;
import io.keel.proto.kv.SnapshotSession;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Per-client request history, which is what turns an at-least-once retry into an exactly-once write.
 *
 * <p>A client that does not hear back cannot tell whether its command was committed, so it retries.
 * Consensus will happily agree the same command twice. For a blind write that is invisible; for a
 * compare-and-swap or a counter increment it is a different outcome, and a linearizability check will
 * find it.
 *
 * <p>Every decision here has to be deterministic, because this table is part of the state machine and
 * every replica must reach the same one. That is why eviction is driven by applied log indexes rather
 * than by wall-clock time or by insertion order: two replicas applying the same log must evict the
 * same sessions.
 */
final class SessionTable {

    /** What a client last did, and what it was told. */
    private record Entry(long sequence, long lastAppliedIndex, CommandResult result) {}

    private final int maxSessions;
    private final Map<Long, Entry> byClient = new HashMap<>();

    SessionTable(int maxSessions) {
        if (maxSessions <= 0) {
            throw new IllegalArgumentException("maxSessions must be positive");
        }
        this.maxSessions = maxSessions;
    }

    /** Registers a client. The id is the log index of the registration, so it needs no counter. */
    void register(long clientId, long index) {
        byClient.put(clientId, new Entry(0, index, CommandResult.getDefaultInstance()));
        evictIfOverCapacity();
    }

    boolean isRegistered(long clientId) {
        return byClient.containsKey(clientId);
    }

    /**
     * The stored answer for a request that has already been applied, if this is one.
     *
     * @return the previous result for a repeat of the most recent sequence number, a rejection for a
     *     sequence older than that, and empty when the request is new
     */
    Optional<CommandResult> replayOf(long clientId, long sequence) {
        Entry entry = byClient.get(clientId);
        if (entry == null) {
            return Optional.empty();
        }
        if (sequence == entry.sequence() && sequence != 0) {
            return Optional.of(entry.result());
        }
        if (sequence < entry.sequence()) {
            // Only the most recent response is kept, so an older sequence cannot be answered. A
            // client issues one request at a time, so this means a badly behaved or resurrected
            // client rather than a normal retry.
            return Optional.of(
                    CommandResult.newBuilder()
                            .setApplied(false)
                            .setMessage(
                                    "sequence "
                                            + sequence
                                            + " is older than the last applied sequence "
                                            + entry.sequence())
                            .build());
        }
        return Optional.empty();
    }

    /** Records the outcome of a newly applied request. */
    void record(long clientId, long sequence, long index, CommandResult result) {
        byClient.put(clientId, new Entry(sequence, index, result));
        evictIfOverCapacity();
    }

    int size() {
        return byClient.size();
    }

    /**
     * Drops the least recently active sessions when over capacity.
     *
     * <p>Ordered by the log index each session was last active at, with the client id breaking ties,
     * so the choice is a function of the log and nothing else.
     */
    private void evictIfOverCapacity() {
        if (byClient.size() <= maxSessions) {
            return;
        }
        List<Map.Entry<Long, Entry>> ordered = new ArrayList<>(byClient.entrySet());
        ordered.sort(
                Comparator.<Map.Entry<Long, Entry>>comparingLong(e -> e.getValue().lastAppliedIndex())
                        .thenComparingLong(Map.Entry::getKey));
        int toRemove = byClient.size() - maxSessions;
        for (int i = 0; i < toRemove; i++) {
            byClient.remove(ordered.get(i).getKey());
        }
    }

    /** Sessions in client id order, so two replicas serialize identical snapshots. */
    List<SnapshotSession> toSnapshot() {
        List<Long> ids = new ArrayList<>(byClient.keySet());
        ids.sort(Comparator.naturalOrder());
        List<SnapshotSession> out = new ArrayList<>(ids.size());
        for (long id : ids) {
            Entry entry = byClient.get(id);
            out.add(
                    SnapshotSession.newBuilder()
                            .setClientId(id)
                            .setSequence(entry.sequence())
                            .setLastAppliedIndex(entry.lastAppliedIndex())
                            .setResult(entry.result())
                            .build());
        }
        return out;
    }

    void restore(List<SnapshotSession> sessions) {
        byClient.clear();
        for (SnapshotSession s : sessions) {
            byClient.put(
                    s.getClientId(),
                    new Entry(s.getSequence(), s.getLastAppliedIndex(), s.getResult()));
        }
    }
}
