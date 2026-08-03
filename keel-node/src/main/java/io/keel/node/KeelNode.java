package io.keel.node;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.keel.kv.MemoryStateMachine;
import io.keel.kv.RocksStateMachine;
import io.keel.kv.StateMachine;
import io.keel.proto.kv.CommandResult;
import io.keel.proto.log.Entry;
import io.keel.proto.log.EntryType;
import io.keel.proto.raft.RaftEnvelope;
import io.keel.proto.raft.RaftServiceGrpc;
import io.keel.proto.raft.SendAck;
import io.keel.raft.LogStore;
import io.keel.raft.NotLeaderException;
import io.keel.raft.RaftConfig;
import io.keel.raft.RaftMessage;
import io.keel.raft.RaftNode;
import io.keel.raft.ReadState;
import io.keel.raft.Ready;
import io.keel.raft.Status;
import io.keel.storage.LogOptions;
import io.keel.storage.SegmentedLog;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A running node: the consensus core, its disk, its state machine, and the network around them.
 *
 * <p>The threading rule is the whole design, and it is short: <b>one thread owns the core</b>. Every
 * call into {@link RaftNode} happens on {@code raftLoop}, so the core needs no locks and cannot be
 * observed halfway through a state transition. Anything arriving from a socket or a client becomes a
 * task on that thread.
 *
 * <p>Applying is a separate thread. A state machine that blocks on a slow disk must not stall
 * replication, and the split also means a leader keeps acknowledging entries while its own applier is
 * behind, which is what Raft permits.
 *
 * <p>Two things the raft thread does that deserve naming. It writes and syncs before releasing a
 * batch's messages, because an acknowledgement is a promise of durability. And it does that
 * synchronously, so a slow disk shows up as latency rather than as a queue that grows until the heap
 * runs out.
 */
public final class KeelNode implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KeelNode.class);

    /** A write waiting for its entry to be applied. */
    private record PendingWrite(ByteString command, CompletableFuture<CommandResult> future) {}

    /** A read waiting for a safe index, and then for the state machine to reach it. */
    private static final class PendingRead {
        final ByteString key;
        final CompletableFuture<ReadAnswer> future;
        volatile long readIndex = -1;

        PendingRead(ByteString key, CompletableFuture<ReadAnswer> future) {
            this.key = key;
            this.future = future;
        }
    }

    /** What a read returned, and the index it was answered at. */
    public record ReadAnswer(Optional<ByteString> value, long readIndex) {}

    /** One peer's channel. */
    private record PeerLink(ManagedChannel channel, RaftServiceGrpc.RaftServiceBlockingStub stub) {}

    private final NodeOptions options;
    private final LogStore store;
    private final StateMachine stateMachine;
    private final RaftNode raft;

    private final ExecutorService raftLoop;
    private final ExecutorService applyLoop;
    private final ExecutorService senders;
    private final ScheduledExecutorService ticker;

    private final Map<Long, PeerLink> peers = new HashMap<>();
    private final LinkedBlockingQueue<Entry> applyQueue = new LinkedBlockingQueue<>();
    private final Map<Long, PendingWrite> pendingWrites = new ConcurrentHashMap<>();
    private final Map<Long, PendingRead> pendingReads = new ConcurrentHashMap<>();
    private final AtomicLong readRequestIds = new AtomicLong();
    private final AtomicBoolean running = new AtomicBoolean();

    private volatile long appliedIndex;
    private Server server;

    private KeelNode(NodeOptions options, LogStore store, StateMachine stateMachine, RaftNode raft) {
        this.options = options;
        this.store = store;
        this.stateMachine = stateMachine;
        this.raft = raft;
        this.raftLoop = Executors.newSingleThreadExecutor(named("keel-raft-" + options.nodeId()));
        this.applyLoop = Executors.newSingleThreadExecutor(named("keel-apply-" + options.nodeId()));
        this.senders = Executors.newFixedThreadPool(4, named("keel-send-" + options.nodeId()));
        this.ticker = Executors.newSingleThreadScheduledExecutor(named("keel-tick-" + options.nodeId()));
    }

    /** Opens a node's log and state machine and rebuilds the core from what is on disk. */
    public static KeelNode open(NodeOptions options) {
        SegmentedLog log = SegmentedLog.open(LogOptions.of(options.dataDir().resolve("wal")));
        StateMachine stateMachine =
                options.stateMachineDir() == null
                        ? new MemoryStateMachine()
                        : RocksStateMachine.open(options.stateMachineDir());
        RaftConfig config =
                RaftConfig.builder(options.nodeId())
                        .voters(options.voters())
                        .electionTimeoutTicks(options.electionTimeoutTicks())
                        .heartbeatTicks(options.heartbeatTicks())
                        .build();
        RaftNode raft = RaftNode.restore(config, log, new Random(options.nodeId() * 7919L));
        return new KeelNode(options, log, stateMachine, raft);
    }

    /** Starts the server, the tick timer, and the apply loop. */
    public KeelNode start() {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("already started");
        }
        for (Map.Entry<Long, String> peer : options.cluster().entrySet()) {
            if (peer.getKey() == options.nodeId()) {
                continue;
            }
            ManagedChannel channel =
                    NettyChannelBuilder.forTarget(peer.getValue()).usePlaintext().build();
            peers.put(
                    peer.getKey(), new PeerLink(channel, RaftServiceGrpc.newBlockingStub(channel)));
        }

        try {
            server =
                    NettyServerBuilder.forPort(options.listenPort())
                            .addService(new RaftTransport())
                            .addService(new KvServiceImpl(this))
                            .build()
                            .start();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to listen on port " + options.listenPort(), e);
        }

        applyLoop.execute(this::applyForever);
        ticker.scheduleAtFixedRate(
                () -> raftLoop.execute(this::tickOnce),
                options.tick().toMillis(),
                options.tick().toMillis(),
                TimeUnit.MILLISECONDS);

        LOG.info(
                "node {} listening on {} with cluster {}",
                options.nodeId(),
                options.cluster().get(options.nodeId()),
                options.cluster().keySet());
        return this;
    }

    // ---------------------------------------------------------------------------------------------
    // Client API
    // ---------------------------------------------------------------------------------------------

    /**
     * Replicates a command and completes when it has been applied.
     *
     * <p>Fails with {@link NotLeaderException} if this node is not the leader, and with
     * {@link OverwrittenException} if a different command ends up at the index this one was assigned,
     * which means it never committed.
     */
    public CompletableFuture<CommandResult> submit(ByteString command) {
        CompletableFuture<CommandResult> future = new CompletableFuture<>();
        raftLoop.execute(
                () -> {
                    try {
                        long index = raft.propose(command.toByteArray());
                        PendingWrite displaced =
                                pendingWrites.put(index, new PendingWrite(command, future));
                        if (displaced != null) {
                            // A previous leader assigned this index to something else and lost it.
                            displaced.future().completeExceptionally(new OverwrittenException(index));
                        }
                        drainReady();
                    } catch (RuntimeException e) {
                        future.completeExceptionally(e);
                    }
                });
        return future;
    }

    /**
     * Reads a key.
     *
     * @param linearizable when true, take a read index from a leader that has confirmed itself with a
     *     quorum and wait for local state to reach it. When false, return whatever this replica holds,
     *     which may be arbitrarily stale.
     */
    public CompletableFuture<ReadAnswer> read(ByteString key, boolean linearizable) {
        if (!linearizable) {
            return CompletableFuture.completedFuture(new ReadAnswer(localRead(key), appliedIndex));
        }
        CompletableFuture<ReadAnswer> future = new CompletableFuture<>();
        long requestId = readRequestIds.incrementAndGet();
        PendingRead pending = new PendingRead(key, future);
        pendingReads.put(requestId, pending);
        future.whenComplete((answer, error) -> pendingReads.remove(requestId));

        raftLoop.execute(
                () -> {
                    try {
                        raft.requestRead(requestId);
                        drainReady();
                    } catch (RuntimeException e) {
                        future.completeExceptionally(e);
                    }
                });
        return future;
    }

    public Status status() {
        try {
            return raftLoop.submit(raft::status).get(options.requestTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while reading status", e);
        } catch (ExecutionException | java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("could not read status", e);
        }
    }

    public long nodeId() {
        return options.nodeId();
    }

    public NodeOptions options() {
        return options;
    }

    public int keyCount() {
        synchronized (stateMachine) {
            return stateMachine.size();
        }
    }

    public long appliedIndex() {
        return appliedIndex;
    }

    // ---------------------------------------------------------------------------------------------
    // The raft thread
    // ---------------------------------------------------------------------------------------------

    private void tickOnce() {
        raft.tick();
        drainReady();
    }

    /**
     * Carries out one {@link Ready} batch, in the order it documents.
     *
     * <p>Runs only on {@code raftLoop}.
     */
    private void drainReady() {
        Ready ready = raft.ready();
        if (ready.isEmpty()) {
            return;
        }
        if (ready.hasHardState()) {
            store.saveHardState(ready.hardState());
        }
        if (!ready.entriesToPersist().isEmpty()) {
            store.append(ready.entriesToPersist());
        }
        // Durable before anything is told about it. A leader counts acknowledgements toward a quorum,
        // so an acknowledgement that a crash could take back is a lie about what is committed.
        store.sync();

        for (RaftMessage message : ready.messages()) {
            dispatch(message);
        }
        for (Entry entry : ready.committedEntries()) {
            applyQueue.add(entry);
        }
        for (ReadState readState : ready.readStates()) {
            PendingRead pending = pendingReads.get(readState.requestId());
            if (pending != null) {
                pending.readIndex = readState.readIndex();
            }
        }
        raft.advance(ready);
        satisfyReads();
    }

    private void dispatch(RaftMessage message) {
        PeerLink link = peers.get(message.to());
        if (link == null) {
            return;
        }
        RaftEnvelope envelope = RaftCodec.toWire(message);
        senders.execute(
                () -> {
                    try {
                        link.stub()
                                .withDeadlineAfter(options.tick().toMillis() * 4, TimeUnit.MILLISECONDS)
                                .send(envelope);
                    } catch (RuntimeException e) {
                        // Raft treats a lost message as a lost message. Retries come from the next
                        // heartbeat, so there is nothing to do but note it.
                        LOG.debug("send to node {} failed: {}", message.to(), e.toString());
                    }
                });
    }

    // ---------------------------------------------------------------------------------------------
    // The apply thread
    // ---------------------------------------------------------------------------------------------

    private void applyForever() {
        while (running.get()) {
            Entry entry;
            try {
                entry = applyQueue.poll(200, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (entry == null) {
                continue;
            }
            try {
                applyOne(entry);
            } catch (RuntimeException e) {
                LOG.error("failed to apply index {}", entry.getIndex(), e);
            }
        }
    }

    private void applyOne(Entry entry) {
        if (entry.getType() == EntryType.ENTRY_TYPE_NORMAL) {
            CommandResult result;
            synchronized (stateMachine) {
                result = stateMachine.apply(entry.getIndex(), entry.getData());
            }
            PendingWrite pending = pendingWrites.remove(entry.getIndex());
            if (pending != null) {
                if (pending.command().equals(entry.getData())) {
                    pending.future().complete(result);
                } else {
                    // Something else took that index, so this write was replaced before it committed.
                    pending.future().completeExceptionally(new OverwrittenException(entry.getIndex()));
                }
            }
        }
        appliedIndex = entry.getIndex();
        satisfyReads();
    }

    /** Answers reads whose index the state machine has reached. Safe to call from either thread. */
    private void satisfyReads() {
        long applied = appliedIndex;
        for (PendingRead pending : pendingReads.values()) {
            long readIndex = pending.readIndex;
            if (readIndex >= 0 && readIndex <= applied) {
                // The index came from a leader that confirmed itself with a quorum, and local state has
                // now caught up to it. Both halves are required; either one alone allows a stale read.
                pending.future.complete(new ReadAnswer(localRead(pending.key), readIndex));
            }
        }
    }

    private Optional<ByteString> localRead(ByteString key) {
        synchronized (stateMachine) {
            return stateMachine.get(key);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Transport
    // ---------------------------------------------------------------------------------------------

    /** Receives consensus messages and hands them to the thread that owns the core. */
    private final class RaftTransport extends RaftServiceGrpc.RaftServiceImplBase {
        @Override
        public void send(RaftEnvelope request, StreamObserver<SendAck> responseObserver) {
            // Acknowledge receipt straight away. The consensus answer, if there is one, travels back as
            // its own message rather than as this call's result.
            responseObserver.onNext(SendAck.getDefaultInstance());
            responseObserver.onCompleted();
            raftLoop.execute(
                    () -> {
                        try {
                            raft.step(RaftCodec.fromWire(request, options.nodeId()));
                            drainReady();
                        } catch (RuntimeException e) {
                            LOG.warn("failed to handle an incoming message", e);
                        }
                    });
        }
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        ticker.shutdownNow();
        if (server != null) {
            server.shutdown();
            try {
                server.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        senders.shutdownNow();
        raftLoop.shutdown();
        applyLoop.shutdown();
        try {
            raftLoop.awaitTermination(5, TimeUnit.SECONDS);
            applyLoop.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        for (PeerLink link : peers.values()) {
            link.channel().shutdownNow();
        }
        peers.clear();
        // Deliberately no sync here: closing is not committing, and anything unsynced was never
        // acknowledged to anyone.
        store.close();
        synchronized (stateMachine) {
            stateMachine.close();
        }
        for (PendingWrite pending : pendingWrites.values()) {
            pending.future().completeExceptionally(new IllegalStateException("node is shutting down"));
        }
        pendingWrites.clear();
        LOG.info("node {} stopped", options.nodeId());
    }

    private static java.util.concurrent.ThreadFactory named(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            return thread;
        };
    }

    /** Thrown when a write's index was taken by another command, so it never committed. */
    public static final class OverwrittenException extends RuntimeException {
        public OverwrittenException(long index) {
            super("index " + index + " was taken by another command; this write did not commit");
        }
    }
}
