/* (C) 2026 */
package com.dalugm.opcdapter.grpc;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.stub.StreamObserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dalugm.opcdapter.api.opcda.v1.ErrorCode;
import com.dalugm.opcdapter.api.opcda.v1.ErrorInfo;
import com.dalugm.opcdapter.api.opcda.v1.PointWriteResult;
import com.dalugm.opcdapter.api.opcda.v1.WriteDisposition;
import com.dalugm.opcdapter.api.opcda.v1.WriteRequest;
import com.dalugm.opcdapter.api.opcda.v1.WriteResponse;
import com.dalugm.opcdapter.api.opcda.v1.WriteTarget;
import com.dalugm.opcdapter.opcda.OpcDaClient;
import com.dalugm.opcdapter.opcda.OpcDaDeadlineException;
import com.dalugm.opcdapter.opcda.OpcDaHresult;

/** Owns write serialization, timeout handling, optional deduplication, and write metrics. */
final class OpcDaWriteHandler {

    record Stats(long writes, long targets, long skipped, long failed) {}

    private static final Logger log = LoggerFactory.getLogger(OpcDaWriteHandler.class);
    private static final long DEFAULT_WRITE_TIMEOUT_MS = 30_000;
    private static final int CONNECTION_LOCK_STRIPES = 64;

    private record PointKey(String connectionId, String itemId) {}

    private record WrittenValue(Object value, long sessionGeneration) {}

    private record IndexedTarget(int index, WriteTarget target) {}

    private final OpcDaClient opcDaClient;
    private final boolean dedupEnabled;
    private final ConcurrentMap<PointKey, WrittenValue> lastWrittenValues =
            new ConcurrentHashMap<>();
    private final ReentrantLock[] connectionLocks = connectionLocks();
    private final ExecutorService executor =
            Executors.newThreadPerTaskExecutor(
                    Thread.ofVirtual().name("opcda-write-", 0).factory());
    private final AtomicLong writes = new AtomicLong();
    private final AtomicLong targets = new AtomicLong();
    private final AtomicLong skipped = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();

    OpcDaWriteHandler(OpcDaClient opcDaClient, boolean dedupEnabled) {
        this.opcDaClient = opcDaClient;
        this.dedupEnabled = dedupEnabled;
    }

    void write(WriteRequest request, StreamObserver<WriteResponse> responseObserver) {
        long deadlineMs = writeDeadlineMs(request);
        List<PointWriteResult> results = new ArrayList<>();
        Map<String, List<IndexedTarget>> byConnection = new LinkedHashMap<>();
        for (int index = 0; index < request.getTargetsCount(); index++) {
            WriteTarget target = request.getTargets(index);
            IndexedTarget indexed = new IndexedTarget(index, target);
            if (target.getConnectionId().isBlank()) {
                results.add(
                        failedWrite(
                                indexed,
                                ErrorCode.ERROR_CODE_INVALID_ARGUMENT,
                                "missing connection_id"));
                continue;
            }
            byConnection
                    .computeIfAbsent(target.getConnectionId(), ignored -> new ArrayList<>())
                    .add(indexed);
        }

        if (deadlineExceeded(deadlineMs)) {
            byConnection.values().stream()
                    .flatMap(List::stream)
                    .forEach(
                            target ->
                                    results.add(
                                            failedWrite(
                                                    target,
                                                    ErrorCode.ERROR_CODE_DEADLINE_EXCEEDED,
                                                    "deadline exceeded before processing")));
        } else {
            for (var entry : byConnection.entrySet()) {
                results.addAll(
                        writeTargets(
                                request.getRequestId(),
                                entry.getKey(),
                                entry.getValue(),
                                deadlineMs));
            }
        }
        results.sort(java.util.Comparator.comparingInt(PointWriteResult::getTargetIndex));
        recordStats(request.getTargetsCount(), results);
        responseObserver.onNext(
                WriteResponse.newBuilder()
                        .setRequestId(request.getRequestId())
                        .addAllResults(results)
                        .setCompletedAtMs(System.currentTimeMillis())
                        .build());
        responseObserver.onCompleted();
    }

    private List<PointWriteResult> writeTargets(
            String requestId, String connectionId, List<IndexedTarget> targets, long deadlineMs) {
        ReentrantLock lock = connectionLock(connectionId);
        boolean acquired = false;
        try {
            if (deadlineMs > 0) {
                long remainingMs = deadlineMs - System.currentTimeMillis();
                acquired = remainingMs > 0 && lock.tryLock(remainingMs, TimeUnit.MILLISECONDS);
            } else {
                lock.lockInterruptibly();
                acquired = true;
            }
            if (!acquired) {
                return targets.stream()
                        .map(
                                target ->
                                        failedWrite(
                                                target,
                                                ErrorCode.ERROR_CODE_DEADLINE_EXCEEDED,
                                                "deadline exceeded while waiting for connection"))
                        .toList();
            }
            return writeTargetsLocked(requestId, connectionId, targets, deadlineMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return targets.stream()
                    .map(
                            target ->
                                    failedWrite(
                                            target,
                                            ErrorCode.ERROR_CODE_INTERNAL,
                                            "interrupted while waiting for connection"))
                    .toList();
        } finally {
            if (acquired) {
                lock.unlock();
            }
        }
    }

    private ReentrantLock connectionLock(String connectionId) {
        return connectionLocks[Math.floorMod(connectionId.hashCode(), connectionLocks.length)];
    }

    private static ReentrantLock[] connectionLocks() {
        ReentrantLock[] locks = new ReentrantLock[CONNECTION_LOCK_STRIPES];
        Arrays.setAll(locks, ignored -> new ReentrantLock(true));
        return locks;
    }

    private List<PointWriteResult> writeTargetsLocked(
            String requestId,
            String connectionId,
            List<IndexedTarget> indexedTargets,
            long deadlineMs) {
        List<PointWriteResult> results = new ArrayList<>();
        Map<String, Object> values = new LinkedHashMap<>();
        Map<String, IndexedTarget> targetsByItem = new LinkedHashMap<>();
        Set<String> seenItemIds = new LinkedHashSet<>();
        for (IndexedTarget indexed : indexedTargets) {
            WriteTarget target = indexed.target();
            String itemId = target.getItemId();
            if (itemId.isBlank()) {
                results.add(
                        failedWrite(
                                indexed, ErrorCode.ERROR_CODE_INVALID_ARGUMENT, "missing item_id"));
                continue;
            }
            if (!seenItemIds.add(itemId)) {
                results.add(
                        failedWrite(
                                indexed,
                                ErrorCode.ERROR_CODE_INVALID_ARGUMENT,
                                "duplicate target in the same request"));
                continue;
            }
            try {
                Object value = extractValue(target);
                PointKey key = new PointKey(connectionId, itemId);
                WrittenValue previous = lastWrittenValues.get(key);
                if (dedupEnabled
                        && previous != null
                        && previous.sessionGeneration()
                                == opcDaClient.sessionGeneration(connectionId)
                        && valuesEqual(value, previous.value())) {
                    results.add(
                            completedWrite(indexed, WriteDisposition.WRITE_DISPOSITION_SKIPPED));
                    continue;
                }
                values.put(itemId, value);
                targetsByItem.put(itemId, indexed);
            } catch (IllegalArgumentException e) {
                results.add(
                        failedWrite(indexed, ErrorCode.ERROR_CODE_INVALID_ARGUMENT, messageOf(e)));
            } catch (Exception e) {
                results.add(
                        failedWrite(
                                indexed,
                                ErrorCode.ERROR_CODE_UNSUPPORTED_VALUE_TYPE,
                                messageOf(e)));
            }
        }

        if (values.isEmpty()) {
            logOutcome(connectionId, requestId, indexedTargets.size(), 0, results);
            return results;
        }
        long remainingMs = remainingMs(deadlineMs);
        if (remainingMs <= 0) {
            targetsByItem
                    .values()
                    .forEach(
                            target ->
                                    results.add(
                                            failedWrite(
                                                    target,
                                                    ErrorCode.ERROR_CODE_DEADLINE_EXCEEDED,
                                                    "deadline exceeded before DCOM write")));
            return results;
        }

        try {
            long generationBeforeWrite = opcDaClient.sessionGeneration(connectionId);
            Future<Map<String, Integer>> future =
                    executor.submit(() -> opcDaClient.write(connectionId, values, deadlineMs));
            Map<String, Integer> writeResults;
            try {
                writeResults = future.get(remainingMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                future.cancel(false);
                targetsByItem
                        .values()
                        .forEach(
                                target ->
                                        results.add(
                                                failedWrite(
                                                        target,
                                                        ErrorCode.ERROR_CODE_DEADLINE_EXCEEDED,
                                                        "deadline exceeded; DCOM write outcome is"
                                                                + " unknown")));
                logOutcome(connectionId, requestId, indexedTargets.size(), values.size(), results);
                return results;
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof Error error) {
                    throw error;
                }
                if (cause instanceof Exception exception) {
                    throw exception;
                }
                throw new RuntimeException(cause == null ? e : cause);
            }

            for (var entry : values.entrySet()) {
                IndexedTarget indexed = targetsByItem.get(entry.getKey());
                Integer hresult = writeResults.get(entry.getKey());
                boolean success = hresult != null && hresult == 0;
                if (success && dedupEnabled) {
                    long generationAfterWrite = opcDaClient.sessionGeneration(connectionId);
                    if (generationBeforeWrite == generationAfterWrite) {
                        lastWrittenValues.put(
                                new PointKey(connectionId, entry.getKey()),
                                new WrittenValue(entry.getValue(), generationAfterWrite));
                    }
                }
                if (success) {
                    results.add(
                            completedWrite(indexed, WriteDisposition.WRITE_DISPOSITION_APPLIED));
                } else {
                    if (hresult == null) {
                        results.add(
                                failedWrite(
                                        indexed,
                                        ErrorCode.ERROR_CODE_OPC_FAILURE,
                                        "OPC server returned no write result"));
                    } else {
                        var known = OpcDaHresult.fromValue(hresult);
                        results.add(
                                failedWrite(
                                        indexed,
                                        errorInfo(
                                                known.map(OpcDaHresult::errorCode)
                                                        .orElse(ErrorCode.ERROR_CODE_OPC_FAILURE),
                                                known.map(OpcDaHresult::message)
                                                        .orElse("OPC write failed"),
                                                hresult)));
                    }
                }
            }
        } catch (Exception e) {
            ErrorCode code =
                    !opcDaClient.hasConnection(connectionId)
                            ? ErrorCode.ERROR_CODE_CONNECTION_NOT_FOUND
                            : OpcDaDeadlineException.causedBy(e)
                                    ? ErrorCode.ERROR_CODE_DEADLINE_EXCEEDED
                                    : ErrorCode.ERROR_CODE_OPC_FAILURE;
            targetsByItem
                    .values()
                    .forEach(target -> results.add(failedWrite(target, code, messageOf(e))));
        }
        logOutcome(connectionId, requestId, indexedTargets.size(), values.size(), results);
        return results;
    }

    private Object extractValue(WriteTarget target) {
        return switch (target.getValueCase()) {
            case BOOL_VALUE -> target.getBoolValue();
            case INT16_VALUE -> {
                int value = target.getInt16Value();
                if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
                    throw new IllegalArgumentException("int16_value is out of range: " + value);
                }
                yield (short) value;
            }
            case INT32_VALUE -> target.getInt32Value();
            case INT64_VALUE -> target.getInt64Value();
            case UINT16_VALUE -> new OpcDaClient.Unsigned16(target.getUint16Value());
            case UINT32_VALUE ->
                    new OpcDaClient.Unsigned32(Integer.toUnsignedLong(target.getUint32Value()));
            case UINT64_VALUE ->
                    throw new UnsupportedOperationException(
                            "uint64 writes are unsupported by J-Interop");
            case FLOAT_VALUE -> target.getFloatValue();
            case DOUBLE_VALUE -> target.getDoubleValue();
            case STRING_VALUE -> target.getStringValue();
            case BYTES_VALUE -> target.getBytesValue().toByteArray();
            case DATETIME_VALUE -> Instant.ofEpochMilli(target.getDatetimeValue());
            default -> throw new IllegalArgumentException("no value set");
        };
    }

    private boolean valuesEqual(Object left, Object right) {
        if (left == right) {
            return true;
        }
        if (left == null || right == null) {
            return false;
        }
        if (left instanceof byte[] leftBytes && right instanceof byte[] rightBytes) {
            return Arrays.equals(leftBytes, rightBytes);
        }
        return left.equals(right);
    }

    private PointWriteResult completedWrite(IndexedTarget indexed, WriteDisposition disposition) {
        return writeResult(indexed)
                .setDisposition(disposition)
                .setCompletedAtMs(System.currentTimeMillis())
                .build();
    }

    private PointWriteResult failedWrite(IndexedTarget indexed, ErrorCode code, String message) {
        return failedWrite(indexed, errorInfo(code, message));
    }

    private PointWriteResult failedWrite(IndexedTarget indexed, ErrorInfo error) {
        return writeResult(indexed)
                .setDisposition(WriteDisposition.WRITE_DISPOSITION_FAILED)
                .setCompletedAtMs(System.currentTimeMillis())
                .setError(error)
                .build();
    }

    private PointWriteResult.Builder writeResult(IndexedTarget indexed) {
        return PointWriteResult.newBuilder()
                .setTargetIndex(indexed.index())
                .setConnectionId(indexed.target().getConnectionId())
                .setItemId(indexed.target().getItemId());
    }

    private ErrorInfo errorInfo(ErrorCode code, String message) {
        return ErrorInfo.newBuilder()
                .setCode(code)
                .setMessage(message == null ? "unknown error" : message)
                .build();
    }

    private ErrorInfo errorInfo(ErrorCode code, String message, int hresult) {
        return ErrorInfo.newBuilder()
                .setCode(code)
                .setMessage(message == null ? "unknown error" : message)
                .setHresult(hresult)
                .build();
    }

    private long writeDeadlineMs(WriteRequest request) {
        long now = System.currentTimeMillis();
        long timeoutMs = request.getTimeoutMs();
        if (timeoutMs < 0) {
            throw new IllegalArgumentException("timeout_ms must not be negative");
        }
        long deadlineMs = saturatingAdd(now, timeoutMs == 0 ? DEFAULT_WRITE_TIMEOUT_MS : timeoutMs);
        Deadline grpcDeadline = Context.current().getDeadline();
        if (grpcDeadline == null) {
            return deadlineMs;
        }
        long grpcRemainingMs = Math.max(0, grpcDeadline.timeRemaining(TimeUnit.MILLISECONDS));
        return Math.min(deadlineMs, saturatingAdd(now, grpcRemainingMs));
    }

    private long saturatingAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

    private long remainingMs(long deadlineMs) {
        return deadlineMs - System.currentTimeMillis();
    }

    private boolean deadlineExceeded(long deadlineMs) {
        return deadlineMs > 0 && System.currentTimeMillis() >= deadlineMs;
    }

    private void recordStats(int targetCount, List<PointWriteResult> results) {
        writes.incrementAndGet();
        targets.addAndGet(targetCount);
        skipped.addAndGet(
                results.stream()
                        .filter(
                                result ->
                                        result.getDisposition()
                                                == WriteDisposition.WRITE_DISPOSITION_SKIPPED)
                        .count());
        failed.addAndGet(
                results.stream()
                        .filter(
                                result ->
                                        result.getDisposition()
                                                == WriteDisposition.WRITE_DISPOSITION_FAILED)
                        .count());
    }

    private void logOutcome(
            String connectionId,
            String requestId,
            int targetCount,
            int sentCount,
            List<PointWriteResult> results) {
        long skippedCount =
                results.stream()
                        .filter(
                                result ->
                                        result.getDisposition()
                                                == WriteDisposition.WRITE_DISPOSITION_SKIPPED)
                        .count();
        long failedCount =
                results.stream()
                        .filter(
                                result ->
                                        result.getDisposition()
                                                == WriteDisposition.WRITE_DISPOSITION_FAILED)
                        .count();
        if (failedCount > 0 || skippedCount > 0) {
            log.info(
                    "write to '{}' [req={}]: {} target(s), {} sent, {} skipped, {} failed",
                    connectionId,
                    requestId,
                    targetCount,
                    sentCount,
                    skippedCount,
                    failedCount);
        } else {
            log.debug(
                    "write to '{}' [req={}]: {} target(s), {} sent",
                    connectionId,
                    requestId,
                    targetCount,
                    sentCount);
        }
    }

    Stats drainStats() {
        return new Stats(
                writes.getAndSet(0),
                targets.getAndSet(0),
                skipped.getAndSet(0),
                failed.getAndSet(0));
    }

    void clearConnection(String connectionId) {
        lastWrittenValues.keySet().removeIf(key -> key.connectionId().equals(connectionId));
    }

    void shutdown() {
        executor.shutdown();
    }

    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return executor.awaitTermination(timeout, unit);
    }

    void shutdownNow() {
        executor.shutdownNow();
    }

    private String messageOf(Throwable error) {
        return error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage();
    }
}
