/* (C) 2026 */
package com.dalugm.opcdapter.grpc;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Value;
import io.micronaut.grpc.annotation.GrpcService;

import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dalugm.opcdapter.api.opcda.v1.AdapterState;
import com.dalugm.opcdapter.api.opcda.v1.ConnectionDataBatch;
import com.dalugm.opcdapter.api.opcda.v1.ConnectionState;
import com.dalugm.opcdapter.api.opcda.v1.ConnectionStatus;
import com.dalugm.opcdapter.api.opcda.v1.ErrorCode;
import com.dalugm.opcdapter.api.opcda.v1.ErrorInfo;
import com.dalugm.opcdapter.api.opcda.v1.GetStatusRequest;
import com.dalugm.opcdapter.api.opcda.v1.GetStatusResponse;
import com.dalugm.opcdapter.api.opcda.v1.ItemValidationResult;
import com.dalugm.opcdapter.api.opcda.v1.OpcDaServiceGrpc;
import com.dalugm.opcdapter.api.opcda.v1.PollAck;
import com.dalugm.opcdapter.api.opcda.v1.PollData;
import com.dalugm.opcdapter.api.opcda.v1.PollRequest;
import com.dalugm.opcdapter.api.opcda.v1.PollResponse;
import com.dalugm.opcdapter.api.opcda.v1.ReadRequest;
import com.dalugm.opcdapter.api.opcda.v1.ReadResponse;
import com.dalugm.opcdapter.api.opcda.v1.ReadSource;
import com.dalugm.opcdapter.api.opcda.v1.SetConnectionsRequest;
import com.dalugm.opcdapter.api.opcda.v1.SetConnectionsResponse;
import com.dalugm.opcdapter.api.opcda.v1.WriteRequest;
import com.dalugm.opcdapter.api.opcda.v1.WriteResponse;
import com.dalugm.opcdapter.opcda.OpcDaClient;
import com.dalugm.opcdapter.opcda.OpcDaDeadlineException;
import com.dalugm.opcdapter.opcda.ReadResult;

@GrpcService
public final class OpcDaServiceImpl extends OpcDaServiceGrpc.OpcDaServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(OpcDaServiceImpl.class);
    private static final long DEFAULT_READ_TIMEOUT_MS = 30_000;
    private static final int MAX_PENDING_CONTROL_RESPONSES = 128;

    private record ConnectionReadResult(ConnectionDataBatch batch, int valueCount) {}

    private record ReadStats(long calls, long values, long failed, long itemErrors, long nonGood) {}

    private ReadStats readStats = new ReadStats(0, 0, 0, 0, 0);

    private final OpcDaClient opcDaClient;
    private final OpcDaWriteHandler writeHandler;
    private final OpcDaDataValueMapper dataValueMapper = new OpcDaDataValueMapper();
    private final Set<PollStreamState> activePollStreams = ConcurrentHashMap.newKeySet();
    private final AtomicLong statPollRounds = new AtomicLong();
    private final AtomicLong statPolledValues = new AtomicLong();
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(
                    2, Thread.ofPlatform().daemon().name("opcda-poll-schedule-", 0).factory());
    private final ExecutorService pollExecutor =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("opcda-poll-", 0).factory());

    @Inject
    public OpcDaServiceImpl(
            OpcDaClient opcDaClient,
            @Value("${opcda.write.dedup.enabled:false}") boolean dedupEnabled) {
        this.opcDaClient = opcDaClient;
        this.writeHandler = new OpcDaWriteHandler(opcDaClient, dedupEnabled);
        scheduler.scheduleAtFixedRate(this::logStats, 60, 60, TimeUnit.SECONDS);
        log.info("OpcDaServiceImpl ready (write dedup: {})", dedupEnabled);
    }

    private void logStats() {
        ReadStats reads = drainReadStats();
        OpcDaWriteHandler.Stats writeStats = writeHandler.drainStats();
        long pollRounds = statPollRounds.getAndSet(0);
        if (reads.calls() == 0 && writeStats.writes() == 0 && pollRounds == 0) {
            return;
        }
        log.info(
                "stats (last 60s) — read: {} call(s), {} value(s), {} failed call(s), {} item"
                    + " error(s), {} non-good quality; poll: {} round(s), {} value(s); write: {}"
                    + " write(s), {} target(s), {} skipped, {} failed",
                reads.calls(),
                reads.values(),
                reads.failed(),
                reads.itemErrors(),
                reads.nonGood(),
                pollRounds,
                statPolledValues.getAndSet(0),
                writeStats.writes(),
                writeStats.targets(),
                writeStats.skipped(),
                writeStats.failed());
    }

    // Count completed, validated unary reads. Batch errors are distinct from
    // item errors and quality observations; none alone proves a DCOM process crash.
    private synchronized void recordReadStats(ConnectionDataBatch batch) {
        long itemErrors = 0;
        long nonGood = 0;
        for (var value : batch.getValuesList()) {
            if (value.hasError()) {
                itemErrors++;
            } else if ((value.getQuality() & 0xC0) != 0xC0) {
                nonGood++;
            }
        }
        readStats =
                new ReadStats(
                        readStats.calls() + 1,
                        readStats.values() + batch.getValuesCount(),
                        readStats.failed() + (batch.hasError() ? 1 : 0),
                        readStats.itemErrors() + itemErrors,
                        readStats.nonGood() + nonGood);
    }

    private synchronized ReadStats drainReadStats() {
        ReadStats snapshot = readStats;
        readStats = new ReadStats(0, 0, 0, 0, 0);
        return snapshot;
    }

    @Override
    public void setConnections(
            SetConnectionsRequest request,
            StreamObserver<SetConnectionsResponse> responseObserver) {
        try {
            Set<String> changed = opcDaClient.setConnections(request.getConnectionsList());
            if (!changed.isEmpty()) {
                for (PollStreamState state : List.copyOf(activePollStreams)) {
                    if (changed.contains(state.connectionId)) {
                        state.closeWithError(
                                Status.UNAVAILABLE
                                        .withDescription("connection configuration changed")
                                        .asRuntimeException());
                        cleanup(state);
                    }
                }
                changed.forEach(writeHandler::clearConnection);
            }
            responseObserver.onNext(
                    SetConnectionsResponse.newBuilder()
                            .setRequestId(request.getRequestId())
                            .build());
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription(messageOf(e)).asRuntimeException());
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription(messageOf(e)).asRuntimeException());
        }
    }

    @Override
    public void getStatus(
            GetStatusRequest request, StreamObserver<GetStatusResponse> responseObserver) {
        List<OpcDaClient.ConnectionHealth> connections = opcDaClient.connectionHealth();
        List<ConnectionStatus> connectionStatuses = new ArrayList<>(connections.size());
        int connected = 0;
        for (OpcDaClient.ConnectionHealth connection : connections) {
            if (connection.connected()) {
                connected++;
            }
            connectionStatuses.add(
                    ConnectionStatus.newBuilder()
                            .setConnectionId(connection.id())
                            .setHost(connection.host())
                            .setState(
                                    connection.connected()
                                            ? ConnectionState.CONNECTION_STATE_CONNECTED
                                            : ConnectionState.CONNECTION_STATE_DISCONNECTED)
                            .setLastSuccessMs(connection.lastSuccessMs())
                            .build());
        }

        AdapterState state;
        if (connections.isEmpty() || connected == connections.size()) {
            state = AdapterState.ADAPTER_STATE_HEALTHY;
        } else if (connected == 0) {
            state = AdapterState.ADAPTER_STATE_DOWN;
        } else {
            state = AdapterState.ADAPTER_STATE_DEGRADED;
        }
        responseObserver.onNext(
                GetStatusResponse.newBuilder()
                        .setState(state)
                        .addAllConnections(connectionStatuses)
                        .build());
        responseObserver.onCompleted();
    }

    @Override
    public void write(WriteRequest request, StreamObserver<WriteResponse> responseObserver) {
        try {
            writeHandler.write(request, responseObserver);
        } catch (IllegalArgumentException e) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription(messageOf(e)).asRuntimeException());
        }
    }

    @Override
    public void read(ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
        try {
            validateReadRequest(request);
            boolean device = request.getSource() == ReadSource.READ_SOURCE_DEVICE;
            long deadlineMs = readDeadlineMs(request);
            ConnectionReadResult result =
                    readConnection(
                            request.getConnectionId(),
                            request.getItemIdsList(),
                            device,
                            deadlineMs);
            recordReadStats(result.batch());
            responseObserver.onNext(
                    ReadResponse.newBuilder()
                            .setRequestId(request.getRequestId())
                            .setBatch(result.batch())
                            .build());
            responseObserver.onCompleted();
        } catch (StatusRuntimeException e) {
            responseObserver.onError(e);
        } catch (IllegalArgumentException e) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription(messageOf(e)).asRuntimeException());
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription(messageOf(e)).asRuntimeException());
        }
    }

    @Override
    public void poll(PollRequest request, StreamObserver<PollResponse> responseObserver) {
        PollStreamState state = new PollStreamState(responseObserver);
        activePollStreams.add(state);
        state.installTransportCallbacks();
        try {
            validatePollRequest(request);
            state.connectionId = request.getConnectionId();
            state.pollIntervalMs = opcDaClient.pollIntervalMs(state.connectionId);

            List<ItemValidationResult> results = new ArrayList<>();
            List<String> candidates = new ArrayList<>();
            Set<String> uniqueItems = new LinkedHashSet<>();
            for (String itemId : request.getItemIdsList()) {
                if (itemId.isBlank() || !uniqueItems.add(itemId)) {
                    results.add(
                            failedValidation(
                                    itemId,
                                    ErrorCode.ERROR_CODE_INVALID_ARGUMENT,
                                    "item_id is missing or duplicated"));
                } else {
                    candidates.add(itemId);
                }
            }

            Set<String> valid = opcDaClient.validateItems(state.connectionId, candidates);
            for (String itemId : candidates) {
                if (valid.contains(itemId)) {
                    state.itemIds.add(itemId);
                    results.add(successfulValidation(itemId));
                } else {
                    results.add(
                            failedValidation(
                                    itemId,
                                    ErrorCode.ERROR_CODE_ITEM_NOT_FOUND,
                                    "OPC item not found"));
                }
            }
            state.enqueue(
                    PollResponse.newBuilder()
                            .setAck(
                                    PollAck.newBuilder()
                                            .setRequestId(request.getRequestId())
                                            .addAllValidations(results))
                            .build());
            if (state.itemIds.isEmpty()) {
                state.closeCompleted();
                cleanup(state);
                return;
            }
            schedulePoll(state, 0);
        } catch (StatusRuntimeException e) {
            state.closeWithError(e);
            cleanup(state);
        } catch (IllegalArgumentException e) {
            state.closeWithError(
                    Status.INVALID_ARGUMENT.withDescription(messageOf(e)).asRuntimeException());
            cleanup(state);
        } catch (Exception e) {
            log.error("Poll handler error", e);
            state.closeWithError(
                    Status.INTERNAL.withDescription(messageOf(e)).asRuntimeException());
            cleanup(state);
        }
    }

    private final class PollStreamState {
        final StreamObserver<PollResponse> responseObserver;
        final ServerCallStreamObserver<?> serverCallObserver;
        final Set<String> itemIds = new LinkedHashSet<>();
        final Object lifecycleLock = new Object();
        final Object outboundLock = new Object();
        final Deque<PollResponse> pendingControlResponses = new ArrayDeque<>();
        final AtomicBoolean cleaned = new AtomicBoolean();
        ScheduledFuture<?> pollFuture;
        PollResponse pendingDataResponse;
        volatile boolean closed;
        String connectionId;
        int pollIntervalMs;
        long generation;
        long sequence;

        PollStreamState(StreamObserver<PollResponse> responseObserver) {
            this.responseObserver = responseObserver;
            serverCallObserver =
                    responseObserver instanceof ServerCallStreamObserver<?> observer
                            ? observer
                            : null;
        }

        void installTransportCallbacks() {
            if (serverCallObserver == null) {
                return;
            }
            serverCallObserver.setOnReadyHandler(this::flushOutbound);
            serverCallObserver.setOnCancelHandler(
                    () -> {
                        markClosed();
                        cleanup(this);
                    });
        }

        void enqueue(PollResponse response) {
            boolean overflow = false;
            synchronized (outboundLock) {
                if (closed) {
                    return;
                }
                if (response.hasData()) {
                    pendingDataResponse = response;
                } else if (pendingControlResponses.size() >= MAX_PENDING_CONTROL_RESPONSES) {
                    overflow = true;
                } else {
                    pendingControlResponses.addLast(response);
                }
            }
            if (overflow) {
                closeWithError(
                        Status.RESOURCE_EXHAUSTED
                                .withDescription("too many pending poll responses")
                                .asRuntimeException());
                cleanup(this);
                return;
            }
            flushOutbound();
        }

        void flushOutbound() {
            RuntimeException failure = null;
            synchronized (outboundLock) {
                if (closed) {
                    return;
                }
                try {
                    while (isTransportReady()) {
                        PollResponse response = pendingControlResponses.pollFirst();
                        if (response == null) {
                            response = pendingDataResponse;
                            pendingDataResponse = null;
                        }
                        if (response == null) {
                            break;
                        }
                        responseObserver.onNext(response);
                    }
                } catch (RuntimeException e) {
                    closed = true;
                    pendingControlResponses.clear();
                    pendingDataResponse = null;
                    failure = e;
                }
            }
            if (failure != null) {
                log.warn("poll send failed, cleaning up stream: {}", messageOf(failure));
                cleanup(this);
            }
        }

        void closeCompleted() {
            synchronized (outboundLock) {
                if (closed) {
                    return;
                }
                closed = true;
                pendingControlResponses.clear();
                pendingDataResponse = null;
                responseObserver.onCompleted();
            }
        }

        void closeWithError(Throwable error) {
            synchronized (outboundLock) {
                if (closed) {
                    return;
                }
                closed = true;
                pendingControlResponses.clear();
                pendingDataResponse = null;
                responseObserver.onError(error);
            }
        }

        void markClosed() {
            synchronized (outboundLock) {
                closed = true;
                pendingControlResponses.clear();
                pendingDataResponse = null;
            }
        }

        boolean isClosed() {
            return closed;
        }

        private boolean isTransportReady() {
            return serverCallObserver == null
                    || (!serverCallObserver.isCancelled() && serverCallObserver.isReady());
        }
    }

    private void validateReadRequest(ReadRequest request) {
        requireRegisteredConnection(request.getConnectionId());
        if (request.getSource() != ReadSource.READ_SOURCE_CACHE
                && request.getSource() != ReadSource.READ_SOURCE_DEVICE) {
            throw new IllegalArgumentException("source must be CACHE or DEVICE");
        }
        if (request.getItemIdsCount() == 0) {
            throw new IllegalArgumentException("at least one item_id is required");
        }
        Set<String> itemIds = new HashSet<>();
        for (String itemId : request.getItemIdsList()) {
            if (itemId.isBlank() || !itemIds.add(itemId)) {
                throw new IllegalArgumentException("item_id is missing or duplicated");
            }
        }
    }

    private void validatePollRequest(PollRequest request) {
        requireRegisteredConnection(request.getConnectionId());
        if (request.getItemIdsCount() == 0) {
            throw new IllegalArgumentException("at least one item_id is required");
        }
    }

    private void requireRegisteredConnection(String connectionId) {
        if (connectionId.isBlank()) {
            throw new IllegalArgumentException("connection_id is required");
        }
        if (!opcDaClient.hasConnection(connectionId)) {
            throw Status.NOT_FOUND
                    .withDescription("unknown connection: '" + connectionId + "'")
                    .asRuntimeException();
        }
    }

    private void schedulePoll(PollStreamState state, long delayMs) {
        synchronized (state.lifecycleLock) {
            if (state.isClosed() || scheduler.isShutdown() || pollExecutor.isShutdown()) {
                return;
            }
            long generation = ++state.generation;
            state.pollFuture =
                    scheduler.schedule(
                            () -> {
                                synchronized (state.lifecycleLock) {
                                    if (state.isClosed() || state.generation != generation) {
                                        return;
                                    }
                                    state.pollFuture = null;
                                }
                                pollExecutor.submit(() -> runPoll(state, generation));
                            },
                            delayMs,
                            TimeUnit.MILLISECONDS);
        }
    }

    private void runPoll(PollStreamState state, long generation) {
        List<String> itemIds;
        synchronized (state.lifecycleLock) {
            if (state.isClosed() || state.generation != generation) {
                return;
            }
            itemIds = List.copyOf(state.itemIds);
        }
        ConnectionReadResult result = readConnection(state.connectionId, itemIds, false, 0);
        long sequence;
        synchronized (state.lifecycleLock) {
            if (state.isClosed() || state.generation != generation) {
                return;
            }
            sequence = ++state.sequence;
        }
        statPollRounds.incrementAndGet();
        statPolledValues.addAndGet(result.valueCount());
        state.enqueue(
                PollResponse.newBuilder()
                        .setData(
                                PollData.newBuilder()
                                        .setSequence(sequence)
                                        .setBatch(result.batch()))
                        .build());
        schedulePoll(state, state.pollIntervalMs);
    }

    private ConnectionReadResult readConnection(
            String connectionId, List<String> itemIds, boolean device, long deadlineMs) {
        long collectedAt = System.currentTimeMillis();
        ConnectionDataBatch.Builder batch =
                ConnectionDataBatch.newBuilder().setConnectionId(connectionId);
        try {
            batch.setHost(opcDaClient.host(connectionId));
            batch.setServerTimeMs(opcDaClient.getServerTimeMs(connectionId, deadlineMs));
            List<ReadResult> reads = opcDaClient.read(connectionId, itemIds, device, deadlineMs);
            collectedAt = System.currentTimeMillis();
            for (ReadResult read : reads) {
                batch.addValues(dataValueMapper.toDataValue(connectionId, read));
            }
            batch.setCollectedAtMs(collectedAt);
            return new ConnectionReadResult(batch.build(), reads.size());
        } catch (Exception e) {
            batch.setCollectedAtMs(collectedAt)
                    .setError(errorInfo(readErrorCode(connectionId, e), messageOf(e)));
            return new ConnectionReadResult(batch.build(), 0);
        }
    }

    private ErrorCode readErrorCode(String connectionId, Exception error) {
        if (!opcDaClient.hasConnection(connectionId)) {
            return ErrorCode.ERROR_CODE_CONNECTION_NOT_FOUND;
        }
        if (error instanceof OpcDaClient.ReadOperationException readFailure) {
            return readFailure.errorCode();
        }
        return OpcDaDeadlineException.causedBy(error)
                ? ErrorCode.ERROR_CODE_DEADLINE_EXCEEDED
                : ErrorCode.ERROR_CODE_OPC_FAILURE;
    }

    private void cleanup(PollStreamState state) {
        if (!state.cleaned.compareAndSet(false, true)) {
            return;
        }
        activePollStreams.remove(state);
        synchronized (state.lifecycleLock) {
            state.generation++;
            state.itemIds.clear();
            if (state.pollFuture != null) {
                state.pollFuture.cancel(false);
                state.pollFuture = null;
            }
        }
    }

    private ItemValidationResult successfulValidation(String itemId) {
        return ItemValidationResult.newBuilder().setItemId(itemId).build();
    }

    private ItemValidationResult failedValidation(String itemId, ErrorCode code, String message) {
        return ItemValidationResult.newBuilder()
                .setItemId(itemId)
                .setError(errorInfo(code, message))
                .build();
    }

    private ErrorInfo errorInfo(ErrorCode code, String message) {
        return ErrorInfo.newBuilder()
                .setCode(code)
                .setMessage(message == null ? "unknown error" : message)
                .build();
    }

    private long readDeadlineMs(ReadRequest request) {
        return rpcDeadlineMs(request.getTimeoutMs(), DEFAULT_READ_TIMEOUT_MS);
    }

    private long rpcDeadlineMs(long configuredTimeoutMs, long defaultTimeoutMs) {
        long now = System.currentTimeMillis();
        if (configuredTimeoutMs < 0) {
            throw new IllegalArgumentException("timeout_ms must not be negative");
        }
        long deadlineMs =
                saturatingAdd(
                        now, configuredTimeoutMs == 0 ? defaultTimeoutMs : configuredTimeoutMs);
        Deadline grpcDeadline = Context.current().getDeadline();
        if (grpcDeadline == null) {
            return deadlineMs;
        }
        long grpcRemainingMs = Math.max(0, grpcDeadline.timeRemaining(TimeUnit.MILLISECONDS));
        long grpcDeadlineMs = saturatingAdd(now, grpcRemainingMs);
        return Math.min(deadlineMs, grpcDeadlineMs);
    }

    private long saturatingAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

    private String messageOf(Throwable error) {
        return error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage();
    }

    @PreDestroy
    void shutdown() {
        for (PollStreamState state : List.copyOf(activePollStreams)) {
            state.closeCompleted();
            cleanup(state);
        }
        scheduler.shutdown();
        pollExecutor.shutdown();
        writeHandler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
            if (!pollExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                pollExecutor.shutdownNow();
            }
            if (!writeHandler.awaitTermination(5, TimeUnit.SECONDS)) {
                writeHandler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            pollExecutor.shutdownNow();
            writeHandler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
