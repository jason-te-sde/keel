package io.keel.node;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import javax.net.ssl.SSLException;
import io.grpc.stub.StreamObserver;
import io.keel.kv.MemoryStateMachine;
import io.keel.kv.RocksStateMachine;
import io.keel.kv.StateMachine;
import io.keel.proto.kv.CommandResult;
import io.keel.proto.log.Entry;
import io.keel.proto.log.EntryType;
import io.keel.proto.log.ConfChange;
import io.keel.proto.log.EntryType;
import io.keel.proto.log.SnapshotMetadata;
import io.keel.proto.raft.InstallSnapshotAck;
import io.keel.proto.raft.InstallSnapshotChunk;
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
import com.google.protobuf.ByteString;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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

    /** One peer's channel, with both a blocking stub and an async one for snapshot streams. */
    private record PeerLink(
            ManagedChannel channel,
            RaftServiceGrpc.RaftServiceBlockingStub stub,
            RaftServiceGrpc.RaftServiceStub asyncStub) {}

    /** Bytes for a snapshot the leader is streaming to a follower, before its metadata is stepped. */
    private static final int SNAPSHOT_CHUNK_BYTES = 256 * 1024;

    private final NodeOptions options;
    private final LogStore store;
    private final StateMachine stateMachine;
    private final RaftNode raft;

    private final ExecutorService raftLoop;
    private final ExecutorService applyLoop;
    private final ExecutorService senders;
    private final ScheduledExecutorService ticker;

    private final Map<Long, PeerLink> peers = new HashMap<>();

    /**
     * Where every known node lives, updated as the membership changes.
     *
     * <p>Addresses arrive in the configuration entries themselves, so a node added while this one was
     * offline is still reachable after a replay: the log is the only place membership comes from.
     */
    private final Map<Long, String> addresses = new java.util.concurrent.ConcurrentHashMap<>();

    private final SnapshotStore snapshots;
    private final NodeMetrics metrics = new NodeMetrics();
    private MetricsServer metricsServer;
    private final LinkedBlockingQueue<Entry> applyQueue = new LinkedBlockingQueue<>();
    private final Map<Long, PendingWrite> pendingWrites = new ConcurrentHashMap<>();
    private final Map<Long, PendingRead> pendingReads = new ConcurrentHashMap<>();
    private final Map<Long, CompletableFuture<java.util.Set<Long>>> pendingMembership =
            new ConcurrentHashMap<>();
    private final AtomicLong readRequestIds = new AtomicLong();
    private final AtomicBoolean running = new AtomicBoolean();

    private volatile long appliedIndex;

    /** Boundary of the newest snapshot this node has taken, so the policy knows when to take another. */
    private volatile long snapshotIndex;

    /** Term of the highest entry applied, needed to label a snapshot. */
    private volatile long lastAppliedTerm;

    private Server server;

    private KeelNode(
            NodeOptions options,
            LogStore store,
            StateMachine stateMachine,
            RaftNode raft,
            SnapshotStore snapshots) {
        this.options = options;
        this.store = store;
        this.stateMachine = stateMachine;
        this.raft = raft;
        this.snapshots = snapshots;
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
        SnapshotStore snapshots = new SnapshotStore(options.dataDir().resolve("snapshots"));

        // The log says where it was compacted to; the state machine has to be brought to the same
        // point before any entry above it is replayed. If the log claims a boundary and no snapshot
        // backs it up, the entries below it are gone and so is their effect: that is data loss, and
        // starting anyway would hide it.
        long boundary = log.snapshotMetadata().getLastIndex();
        long restoredAt = 0;
        if (boundary > 0) {
            SnapshotStore.Stored stored =
                    snapshots
                            .latest()
                            .filter(candidate -> candidate.meta().getLastIndex() >= boundary)
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "the log was compacted to index "
                                                            + boundary
                                                            + " but no snapshot covering it exists in "
                                                            + options.dataDir().resolve("snapshots")));
            stateMachine.restore(new ByteArrayInputStream(snapshots.read(stored)));
            restoredAt = stored.meta().getLastIndex();
            LOG.info("node {} restored its state machine from a snapshot at {}", options.nodeId(), restoredAt);
        }

        KeelNode node = new KeelNode(options, log, stateMachine, raft, snapshots);
        node.appliedIndex = restoredAt;
        node.snapshotIndex = restoredAt;
        return node;
    }

    /** Starts the server, the tick timer, and the apply loop. */
    public KeelNode start() {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("already started");
        }
        addresses.putAll(options.cluster());
        for (long peer : options.cluster().keySet()) {
            if (peer != options.nodeId()) {
                connect(peer);
            }
        }

        SecurityOptions security = options.security();
        // Refuses to start rather than starting insecurely. An unauthenticated store reachable from a
        // network is the kind of mistake that is only discovered by someone else.
        security.checkUsableOn(options.listenHost());

        try {
            NettyServerBuilder builder =
                    NettyServerBuilder.forPort(options.listenPort())
                            // A request larger than this is refused with a reason, instead of failing
                            // inside gRPC's default limit in a way that says nothing useful.
                            .maxInboundMessageSize(options.maxRequestBytes())
                            // The peer protocol is guarded by mutual TLS, not by a token: an
                            // unauthorised process fails the handshake and never sends a Raft message.
                            .addService(new RaftTransport())
                            .addService(
                                    ServerInterceptors.intercept(
                                            new KvServiceImpl(this),
                                            new AuthInterceptor(
                                                    security.clientToken(), security.adminToken())));
            if (security.tlsEnabled()) {
                builder.sslContext(
                        GrpcSslContexts.configure(
                                        io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder
                                                .forServer(
                                                        security.certificate().toFile(),
                                                        security.privateKey().toFile())
                                                .trustManager(security.trustedCa().toFile())
                                                // Certificates are the membership boundary, so a peer
                                                // without one does not get to talk at all.
                                                .clientAuth(ClientAuth.REQUIRE))
                                .build());
            }
            server = builder.build().start();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to listen on port " + options.listenPort(), e);
        }

        if (options.metricsPort() > 0) {
            metricsServer = new MetricsServer(this, metrics, options.metricsPort()).start();
        }

        applyLoop.execute(this::applyForever);
        ticker.scheduleAtFixedRate(
                () -> raftLoop.execute(this::tickOnce),
                options.tick().toMillis(),
                options.tick().toMillis(),
                TimeUnit.MILLISECONDS);

        LOG.info(
                "node {} listening on {} with cluster {}, {}",
                options.nodeId(),
                options.cluster().get(options.nodeId()),
                options.cluster().keySet(),
                security);
        return this;
    }

    /** Opens a channel to a peer, if one is not open already. */
    private synchronized void connect(long peer) {
        if (peers.containsKey(peer)) {
            return;
        }
        String address = addresses.get(peer);
        if (address == null) {
            LOG.warn("no address known for node {}; cannot connect", peer);
            return;
        }
        ManagedChannel channel = channelTo(address);
        peers.put(
                peer,
                new PeerLink(
                        channel,
                        RaftServiceGrpc.newBlockingStub(channel),
                        RaftServiceGrpc.newStub(channel)));
        LOG.info("node {} connected to node {} at {}", options.nodeId(), peer, address);
    }

    /**
     * Opens a channel to {@code address}, with mutual TLS when it is configured.
     *
     * <p>The node presents its own certificate here as well as verifying the peer's, so authentication
     * runs in both directions: a follower checks that the leader belongs to the cluster just as the
     * leader checks the follower.
     */
    private ManagedChannel channelTo(String address) {
        SecurityOptions security = options.security();
        if (!security.tlsEnabled()) {
            return NettyChannelBuilder.forTarget(address).usePlaintext().build();
        }
        try {
            SslContext ssl =
                    GrpcSslContexts.forClient()
                            .keyManager(
                                    security.certificate().toFile(), security.privateKey().toFile())
                            .trustManager(security.trustedCa().toFile())
                            .build();
            return NettyChannelBuilder.forTarget(address).sslContext(ssl).build();
        } catch (SSLException e) {
            throw new IllegalStateException("failed to build a TLS channel to " + address, e);
        }
    }

    private synchronized void disconnect(long peer) {
        PeerLink link = peers.remove(peer);
        if (link != null) {
            link.channel().shutdownNow();
            LOG.info("node {} disconnected from node {}", options.nodeId(), peer);
        }
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
                        metrics.writeAccepted();
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
     * Proposes a membership change and completes when it has been applied.
     *
     * <p>One at a time: a second change offered while the first is unapplied is refused, because two in
     * flight can produce two disjoint majorities and therefore two leaders in one term.
     */
    public CompletableFuture<java.util.Set<Long>> changeMembership(ConfChange change) {
        CompletableFuture<java.util.Set<Long>> future = new CompletableFuture<>();
        raftLoop.execute(
                () -> {
                    try {
                        if (change.getType() == ConfChange.Type.TYPE_ADD_VOTER
                                && !change.getAddress().isEmpty()) {
                            addresses.put(change.getNodeId(), change.getAddress());
                        }
                        long index = raft.proposeConfChange(change);
                        pendingMembership.put(index, future);
                        drainReady();
                    } catch (RuntimeException e) {
                        metrics.writeRejected();
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
            metrics.readServed();
            return CompletableFuture.completedFuture(new ReadAnswer(localRead(key), appliedIndex));
        }
        CompletableFuture<ReadAnswer> future = new CompletableFuture<>();
        long requestId = readRequestIds.incrementAndGet();
        PendingRead pending = new PendingRead(key, future);
        pendingReads.put(requestId, pending);
        future.whenComplete((answer, error) -> pendingReads.remove(requestId));

        future.whenComplete(
                (answer, error) -> {
                    if (error == null) {
                        metrics.readServed();
                    } else {
                        metrics.readFailed();
                    }
                });
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

    /** Port the metrics endpoints are on, or 0 when they are disabled. */
    public int metricsPort() {
        return metricsServer == null ? 0 : metricsServer.port();
    }

    /** Boundary of the newest snapshot this node has taken or installed, or 0 if it has none. */
    public long snapshotIndex() {
        return snapshotIndex;
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
        // First, because the entries in this same batch are the ones that follow the snapshot's
        // boundary. Appending them into a log that still starts lower down would leave a gap.
        if (ready.hasSnapshotToInstall()) {
            installSnapshot(ready.snapshotToInstall());
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
        if (message instanceof RaftMessage.InstallSnapshot snapshot) {
            streamSnapshot(link, snapshot);
            return;
        }
        if (message instanceof RaftMessage.InstallSnapshotReply) {
            // The reply is the stream's return value, produced by the receiving node's own core and
            // then discarded here. Sending it as a separate message would let the leader believe a
            // follower holds a snapshot before the payload has landed.
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
                        // heartbeat, so there is nothing to do but count it.
                        metrics.peerSendFailed();
                        LOG.debug("send to node {} failed: {}", message.to(), e.toString());
                    }
                });
    }

    /**
     * Streams a snapshot payload to a follower and steps the outcome back into the core.
     *
     * <p>Runs off the raft thread: a snapshot is large and a follower may be slow, and stalling the
     * thread that owns the core would stall the whole node.
     */
    private void streamSnapshot(PeerLink link, RaftMessage.InstallSnapshot message) {
        senders.execute(
                () -> {
                    boolean landed = false;
                    long matchIndex = 0;
                    try {
                        SnapshotStore.Stored stored =
                                snapshots
                                        .latest()
                                        .filter(s -> s.meta().getLastIndex() >= message.meta().getLastIndex())
                                        .orElseThrow(
                                                () ->
                                                        new IllegalStateException(
                                                                "no snapshot on disk covering index "
                                                                        + message.meta().getLastIndex()));
                        byte[] payload = snapshots.read(stored);
                        InstallSnapshotAck ack = sendChunks(link, message, stored.meta(), payload);
                        landed = ack.getSuccess();
                        matchIndex = ack.getMatchIndex();
                        metrics.snapshotSent();
                    } catch (RuntimeException e) {
                        metrics.snapshotSendFailed();
                        LOG.warn("snapshot to node {} failed: {}", message.to(), e.toString());
                    }
                    long finalMatch = matchIndex;
                    boolean finalLanded = landed;
                    raftLoop.execute(
                            () -> {
                                raft.step(
                                        new RaftMessage.InstallSnapshotReply(
                                                message.to(),
                                                options.nodeId(),
                                                message.term(),
                                                finalLanded,
                                                finalMatch));
                                drainReady();
                            });
                });
    }

    private InstallSnapshotAck sendChunks(
            PeerLink link, RaftMessage.InstallSnapshot message, SnapshotMetadata meta, byte[] payload) {
        java.util.concurrent.CompletableFuture<InstallSnapshotAck> answer = new CompletableFuture<>();
        StreamObserver<InstallSnapshotChunk> stream =
                link.asyncStub()
                        .withDeadlineAfter(options.requestTimeout().toMillis() * 4, TimeUnit.MILLISECONDS)
                        .installSnapshot(
                                new StreamObserver<>() {
                                    @Override
                                    public void onNext(InstallSnapshotAck ack) {
                                        answer.complete(ack);
                                    }

                                    @Override
                                    public void onError(Throwable error) {
                                        answer.completeExceptionally(error);
                                    }

                                    @Override
                                    public void onCompleted() {
                                        answer.complete(InstallSnapshotAck.getDefaultInstance());
                                    }
                                });
        try {
            for (int offset = 0; offset < Math.max(payload.length, 1); offset += SNAPSHOT_CHUNK_BYTES) {
                int end = Math.min(offset + SNAPSHOT_CHUNK_BYTES, payload.length);
                stream.onNext(
                        InstallSnapshotChunk.newBuilder()
                                .setTerm(message.term())
                                .setLeaderId(options.nodeId())
                                .setMeta(meta)
                                .setOffset(offset)
                                .setData(ByteString.copyFrom(payload, offset, end - offset))
                                .setLast(end >= payload.length)
                                .build());
            }
            stream.onCompleted();
            return answer.get(options.requestTimeout().toMillis() * 4, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while sending a snapshot", e);
        } catch (ExecutionException | java.util.concurrent.TimeoutException e) {
            stream.onError(e);
            throw new IllegalStateException("snapshot stream failed", e);
        }
    }

    /** Installs a snapshot the core has accepted. Runs on the raft thread. */
    private void installSnapshot(SnapshotMetadata meta) {
        byte[] payload = receivedSnapshot;
        if (payload == null) {
            throw new IllegalStateException(
                    "the core accepted a snapshot at " + meta.getLastIndex() + " with no payload received");
        }
        receivedSnapshot = null;
        synchronized (stateMachine) {
            stateMachine.restore(new ByteArrayInputStream(payload));
        }
        store.installSnapshot(meta);
        appliedIndex = meta.getLastIndex();
        snapshotIndex = meta.getLastIndex();
        lastAppliedTerm = meta.getLastTerm();
        metrics.snapshotInstalled();
        LOG.info("node {} installed a snapshot at index {}", options.nodeId(), meta.getLastIndex());
        satisfyReads();
    }

    /**
     * Takes a snapshot and compacts the log once it has grown past the threshold.
     *
     * <p>The state machine is serialized first and the log compacted second, both because the snapshot
     * has to be durable before the entries it replaces are dropped, and because compaction happens on
     * the raft thread while snapshotting happens here.
     */
    private void maybeSnapshot() {
        int threshold = options.snapshotThresholdEntries();
        if (threshold == 0 || appliedIndex - snapshotIndex < threshold) {
            return;
        }
        long boundary = appliedIndex;
        long boundaryTerm = lastAppliedTerm;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        synchronized (stateMachine) {
            stateMachine.snapshot(out);
        }
        SnapshotMetadata meta =
                snapshots.write(
                        SnapshotMetadata.newBuilder()
                                .setLastIndex(boundary)
                                .setLastTerm(boundaryTerm)
                                // A node restoring from this cannot recover the membership from the
                                // log: the entries that carried it are what the snapshot replaced.
                                .setConf(raft.confState())
                                .build(),
                        out.toByteArray());
        snapshotIndex = boundary;
        metrics.snapshotTaken();
        raftLoop.execute(
                () -> {
                    try {
                        store.compact(meta);
                        store.sync();
                    } catch (RuntimeException e) {
                        // Failing to compact costs disk, not correctness: the snapshot is already
                        // durable and the entries it covers are still there.
                        LOG.warn("could not compact to index {}: {}", meta.getLastIndex(), e.toString());
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
                    metrics.writeOverwritten();
                    pending.future().completeExceptionally(new OverwrittenException(entry.getIndex()));
                }
            }
        }
        if (entry.getType() == EntryType.ENTRY_TYPE_CONF_CHANGE) {
            applyConfChange(entry);
            metrics.membershipChanged();
        }
        metrics.entryApplied();
        appliedIndex = entry.getIndex();
        lastAppliedTerm = entry.getTerm();
        satisfyReads();
        maybeSnapshot();
    }

    /**
     * Puts a committed membership change into effect.
     *
     * <p>The core has to be told on its own thread, and the transport has to follow: a node that has
     * just been added needs a channel, and one that has been removed should not keep one.
     */
    private void applyConfChange(Entry entry) {
        ConfChange change = RaftNode.decodeConfChange(entry);
        if (change.getType() == ConfChange.Type.TYPE_ADD_VOTER && !change.getAddress().isEmpty()) {
            addresses.put(change.getNodeId(), change.getAddress());
        }
        raftLoop.execute(
                () -> {
                    java.util.Set<Long> voters;
                    try {
                        voters = new java.util.TreeSet<>(raft.applyConfChange(change).getVotersList());
                    } catch (RuntimeException e) {
                        LOG.error("failed to apply a membership change at index {}", entry.getIndex(), e);
                        return;
                    }
                    if (change.getType() == ConfChange.Type.TYPE_ADD_VOTER
                            && change.getNodeId() != options.nodeId()) {
                        connect(change.getNodeId());
                    } else if (change.getType() == ConfChange.Type.TYPE_REMOVE_VOTER) {
                        disconnect(change.getNodeId());
                    }
                    CompletableFuture<java.util.Set<Long>> waiting =
                            pendingMembership.remove(entry.getIndex());
                    if (waiting != null) {
                        waiting.complete(voters);
                    }
                    drainReady();
                });
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

    /** Payload of a snapshot received but not yet accepted by the core. */
    private volatile byte[] receivedSnapshot;

    /** Receives consensus messages and hands them to the thread that owns the core. */
    private final class RaftTransport extends RaftServiceGrpc.RaftServiceImplBase {

        @Override
        public StreamObserver<InstallSnapshotChunk> installSnapshot(
                StreamObserver<InstallSnapshotAck> responseObserver) {
            return new StreamObserver<>() {
                private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                private SnapshotMetadata meta;

                @Override
                public void onNext(InstallSnapshotChunk chunk) {
                    meta = chunk.getMeta();
                    byte[] data = chunk.getData().toByteArray();
                    buffer.write(data, 0, data.length);
                }

                @Override
                public void onError(Throwable error) {
                    LOG.warn("a snapshot stream failed: {}", error.toString());
                }

                @Override
                public void onCompleted() {
                    if (meta == null) {
                        responseObserver.onNext(InstallSnapshotAck.getDefaultInstance());
                        responseObserver.onCompleted();
                        return;
                    }
                    SnapshotMetadata received = meta;
                    byte[] payload = buffer.toByteArray();
                    // Durable and verified before the core is told about it, so the acknowledgement
                    // means installed rather than merely received.
                    SnapshotMetadata stored;
                    try {
                        stored = snapshots.accept(received, payload);
                    } catch (RuntimeException e) {
                        LOG.warn("rejecting a snapshot: {}", e.toString());
                        responseObserver.onNext(
                                InstallSnapshotAck.newBuilder()
                                        .setFollowerId(options.nodeId())
                                        .setSuccess(false)
                                        .build());
                        responseObserver.onCompleted();
                        return;
                    }
                    receivedSnapshot = payload;
                    raftLoop.execute(
                            () -> {
                                try {
                                    raft.step(
                                            new RaftMessage.InstallSnapshot(
                                                    received.getLastIndex() == 0 ? 0 : receivedFrom(received),
                                                    options.nodeId(),
                                                    Math.max(received.getLastTerm(), raft.term()),
                                                    stored));
                                    drainReady();
                                    responseObserver.onNext(
                                            InstallSnapshotAck.newBuilder()
                                                    .setTerm(raft.term())
                                                    .setFollowerId(options.nodeId())
                                                    .setSuccess(true)
                                                    .setMatchIndex(Math.max(raft.commitIndex(), stored.getLastIndex()))
                                                    .build());
                                } catch (RuntimeException e) {
                                    LOG.warn("failed to install a snapshot", e);
                                    responseObserver.onNext(
                                            InstallSnapshotAck.newBuilder()
                                                    .setFollowerId(options.nodeId())
                                                    .setSuccess(false)
                                                    .build());
                                }
                                responseObserver.onCompleted();
                            });
                }
            };
        }

        /** The leader this snapshot came from, which is whoever the node currently follows. */
        private long receivedFrom(SnapshotMetadata meta) {
            long leader = raft.leaderId();
            return leader == 0 ? options.nodeId() : leader;
        }
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
        if (metricsServer != null) {
            metricsServer.close();
        }
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
        for (CompletableFuture<java.util.Set<Long>> pending : pendingMembership.values()) {
            pending.completeExceptionally(new IllegalStateException("node is shutting down"));
        }
        pendingMembership.clear();
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
