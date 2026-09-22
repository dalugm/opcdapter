/* (C) 2026 */
package com.dalugm.opcdapter.opcda;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import io.micronaut.context.annotation.Value;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.jinterop.dcom.common.JIException;
import org.jinterop.dcom.core.JIArray;
import org.jinterop.dcom.core.JIFlags;
import org.jinterop.dcom.core.JIUnsignedFactory;
import org.jinterop.dcom.core.JIVariant;
import org.openscada.opc.lib.common.ConnectionInformation;
import org.openscada.opc.lib.da.ItemState;
import org.openscada.opc.lib.da.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dalugm.opcdapter.api.opcda.v1.Connection;
import com.dalugm.opcdapter.api.opcda.v1.ErrorCode;

@Singleton
public class OpcDaClient {

    private static final Logger log = LoggerFactory.getLogger(OpcDaClient.class);
    private static final long SERVER_TIME_RECALIBRATE_MS = 300_000;
    private static final int SESSION_LOCK_STRIPES = 64;

    private record ServerTimeCalibration(long offsetMs, long measuredAtMs) {}

    /** A whole-read failure that belongs on ConnectionDataBatch rather than every item. */
    public static final class ReadOperationException extends RuntimeException {
        private final ErrorCode errorCode;

        public ReadOperationException(ErrorCode errorCode, String message) {
            super(message);
            this.errorCode = errorCode;
        }

        ReadOperationException(ErrorCode errorCode, String message, Throwable cause) {
            super(message, cause);
            this.errorCode = errorCode;
        }

        public ErrorCode errorCode() {
            return errorCode;
        }
    }

    public record ConnectionHealth(String id, String host, boolean connected, long lastSuccessMs) {}

    public record Unsigned16(int value) {
        public Unsigned16 {
            if (value < 0 || value > 0xFFFF) {
                throw new IllegalArgumentException("uint16 out of range: " + value);
            }
        }
    }

    public record Unsigned32(long value) {
        public Unsigned32 {
            if (value < 0 || value > 0xFFFF_FFFFL) {
                throw new IllegalArgumentException("uint32 out of range: " + value);
            }
        }
    }

    private final ConcurrentMap<String, Connection> connections = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, OpcDaSession> sessions = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<String>> knownItemsByConnection =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> lastSuccessByConnection = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ServerTimeCalibration> serverTimeCalibration =
            new ConcurrentHashMap<>();
    private final ReentrantLock[] sessionLocks = createSessionLocks();
    private final ConcurrentMap<String, AtomicLong> sessionGenerations = new ConcurrentHashMap<>();
    private final Set<String> retiringConnections = ConcurrentHashMap.newKeySet();
    private final DcomExecutor dcom;

    @Value("${opcda.dcom.socket-timeout-ms:5000}")
    private int socketTimeoutMs = 5_000;

    public OpcDaClient(int dcomThreads) {
        this(dcomThreads, Math.max(16, dcomThreads * 4));
    }

    @Inject
    public OpcDaClient(
            @Value("${opcda.dcom.threads:16}") int dcomThreads,
            @Value("${opcda.dcom.queue-capacity:64}") int dcomQueueCapacity) {
        if (dcomThreads <= 0) {
            throw new IllegalArgumentException(
                    "opcda.dcom.threads must be > 0, got " + dcomThreads);
        }
        if (dcomQueueCapacity < 0) {
            throw new IllegalArgumentException(
                    "opcda.dcom.queue-capacity must be >= 0, got " + dcomQueueCapacity);
        }
        dcom = new DcomExecutor(dcomThreads, dcomQueueCapacity);
        log.info(
                "OpcDaClient ready (DCOM threads: {}, queue capacity: {})",
                dcomThreads,
                dcomQueueCapacity);
    }

    @PostConstruct
    void init() {
        if (socketTimeoutMs <= 0) {
            throw new IllegalArgumentException(
                    "opcda.dcom.socket-timeout-ms must be > 0, got " + socketTimeoutMs);
        }
        // Utgard 1.5.0 reads this key in Server.connect() and copies it into JISession.
        System.setProperty("rpc.socketTimeout", Integer.toString(socketTimeoutMs));
    }

    /** Replaces the desired connection set after validating every entry. */
    public synchronized Set<String> setConnections(Collection<Connection> desiredConnections) {
        Map<String, Connection> desired = new LinkedHashMap<>();
        for (Connection connection : desiredConnections) {
            validateConnection(connection);
            if (desired.putIfAbsent(connection.getId(), connection) != null) {
                throw new IllegalArgumentException(
                        "duplicate connection id: '" + connection.getId() + "'");
            }
        }

        Set<String> changed = new LinkedHashSet<>();
        for (var existing : connections.entrySet()) {
            Connection replacement = desired.get(existing.getKey());
            if (!existing.getValue().equals(replacement)) {
                changed.add(existing.getKey());
            }
        }
        for (var entry : desired.entrySet()) {
            if (!entry.getValue().equals(connections.get(entry.getKey()))) {
                changed.add(entry.getKey());
            }
        }

        for (String connectionId : changed) {
            removeConnection(connectionId);
        }
        for (var entry : desired.entrySet()) {
            if (!connections.containsKey(entry.getKey())) {
                connections.put(entry.getKey(), entry.getValue());
                log.info(
                        "connection '{}' ({}) registered",
                        entry.getKey(),
                        entry.getValue().getHost());
            }
        }
        return Set.copyOf(changed);
    }

    public List<String> connectionIds() {
        return connections.keySet().stream().sorted().toList();
    }

    /** Returns one consistent registry snapshot for status reporting. */
    public synchronized List<ConnectionHealth> connectionHealth() {
        long now = System.currentTimeMillis();
        return connections.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(
                        entry -> {
                            String connectionId = entry.getKey();
                            long lastSuccess =
                                    lastSuccessByConnection.getOrDefault(connectionId, 0L);
                            return new ConnectionHealth(
                                    connectionId,
                                    entry.getValue().getHost(),
                                    isConnected(connectionId, entry.getValue(), lastSuccess, now),
                                    lastSuccess);
                        })
                .toList();
    }

    public boolean hasConnection(String connectionId) {
        return connections.containsKey(requireConnectionId(connectionId));
    }

    public String host(String connectionId) {
        Connection connection = connections.get(requireConnectionId(connectionId));
        if (connection == null) {
            throw new IllegalArgumentException("unknown connection: '" + connectionId + "'");
        }
        return connection.getHost();
    }

    public int pollIntervalMs(String connectionId) {
        Connection connection = connections.get(requireConnectionId(connectionId));
        if (connection == null) {
            throw new IllegalArgumentException("unknown connection: '" + connectionId + "'");
        }
        return pollIntervalMillis(connection);
    }

    public Set<String> validateItems(String connectionId, Collection<String> itemIds) {
        if (itemIds.isEmpty()) {
            return Set.of();
        }
        requireRegistered(connectionId);
        rememberItems(connectionId, itemIds);
        try {
            OpcDaSession session = session(connectionId);
            long timeoutMs = dcomTimeoutMs(0);
            long startBeforeMs = startBefore(timeoutMs);
            return dcom.execute(
                    () -> session.ensureItems(itemIds, startBeforeMs),
                    timeoutMs,
                    "validateItems '" + connectionId + "'");
        } catch (Exception e) {
            log.warn(
                    "validateItems failed on '{}', accepting all items optimistically: {}",
                    connectionId,
                    messageOf(e));
            return new LinkedHashSet<>(itemIds);
        }
    }

    public List<ReadResult> read(String connectionId, List<String> itemIds) {
        return read(connectionId, itemIds, false, 0);
    }

    public List<ReadResult> read(
            String connectionId, List<String> itemIds, boolean device, long deadlineMs) {
        if (itemIds.isEmpty()) {
            return List.of();
        }
        requireRegistered(connectionId);
        ensureBeforeDeadline(deadlineMs);
        rememberItems(connectionId, itemIds);
        try {
            return readAttempts(connectionId, itemIds, device, deadlineMs);
        } catch (DcomExecutor.DcomTimeoutException
                | OpcDaSession.OperationStartTimeoutException e) {
            log.warn("read timed out on '{}': {}", connectionId, e.getMessage());
            throw new ReadOperationException(
                    ErrorCode.ERROR_CODE_DEADLINE_EXCEEDED, e.getMessage(), e);
        } catch (OpcDaDeadlineException e) {
            log.warn("read deadline exceeded on '{}': {}", connectionId, e.getMessage());
            throw new ReadOperationException(
                    ErrorCode.ERROR_CODE_DEADLINE_EXCEEDED, e.getMessage(), e);
        } catch (RejectedExecutionException e) {
            // Overload is local backpressure, not evidence that the OPC session is unhealthy.
            log.warn("read rejected on '{}': {}", connectionId, messageOf(e));
            throw new ReadOperationException(ErrorCode.ERROR_CODE_OPC_FAILURE, messageOf(e), e);
        } catch (Exception e) {
            ErrorCode code =
                    OpcDaDeadlineException.causedBy(e)
                            ? ErrorCode.ERROR_CODE_DEADLINE_EXCEEDED
                            : ErrorCode.ERROR_CODE_OPC_FAILURE;
            throw new ReadOperationException(code, messageOf(e), e);
        }
    }

    List<ReadResult> readAttempts(
            String connectionId, List<String> itemIds, boolean device, long deadlineMs)
            throws Exception {
        OpcDaSession initial = null;
        try {
            initial = session(connectionId, deadlineMs);
            return read0(connectionId, initial, itemIds, device, deadlineMs);
        } catch (OpcDaDeadlineException | RejectedExecutionException e) {
            throw e;
        } catch (Exception firstError) {
            log.warn("read failed on '{}', reconnecting: {}", connectionId, messageOf(firstError));
            OpcDaSession replacement = reconnectAfterFailure(connectionId, initial, deadlineMs);
            ensureBeforeDeadline(deadlineMs);
            try {
                return read0(connectionId, replacement, itemIds, device, deadlineMs);
            } catch (Exception retryError) {
                throw new RuntimeException(
                        "read failed after retry: " + messageOf(retryError), retryError);
            }
        }
    }

    private List<ReadResult> read0(
            String connectionId,
            OpcDaSession session,
            List<String> itemIds,
            boolean device,
            long deadlineMs)
            throws Exception {
        long startedAt = System.currentTimeMillis();
        long timeoutMs = dcomTimeoutMs(deadlineMs);
        long startBeforeMs = startBefore(timeoutMs);
        Map<String, ItemState> states =
                dcom.execute(
                        () -> session.read(itemIds, device, startBeforeMs),
                        timeoutMs,
                        "read '" + connectionId + "'");
        lastSuccessByConnection.put(connectionId, System.currentTimeMillis());
        log.debug(
                "read '{}' completed in {}ms ({} items)",
                connectionId,
                elapsedSince(startedAt),
                itemIds.size());

        List<ReadResult> results = new ArrayList<>(itemIds.size());
        for (String id : itemIds) {
            results.add(toReadResult(id, states.get(id)));
        }
        return results;
    }

    ReadResult toReadResult(String itemId, ItemState state) {
        if (state == null) {
            return failedRead(
                    itemId,
                    0,
                    0,
                    ErrorCode.ERROR_CODE_OPC_FAILURE,
                    "OPC server returned no item state",
                    null);
        }

        int quality = state.getQuality() == null ? 0 : Short.toUnsignedInt(state.getQuality());
        long timestamp = state.getTimestamp() == null ? 0L : state.getTimestamp().getTimeInMillis();
        if (state.getErrorCode() != 0) {
            int hresult = state.getErrorCode();
            var known = OpcDaHresult.fromValue(hresult);
            ErrorCode code =
                    known.map(OpcDaHresult::errorCode).orElse(ErrorCode.ERROR_CODE_OPC_FAILURE);
            String message = known.map(OpcDaHresult::message).orElse("OPC read failed");
            return failedRead(itemId, quality, timestamp, code, message, hresult);
        }

        try {
            JIVariant variant = state.getValue();
            if (variant == null || isEmptyVariant(variant)) {
                return failedRead(
                        itemId,
                        quality,
                        timestamp,
                        ErrorCode.ERROR_CODE_NO_VALUE,
                        "OPC item has no current value",
                        null);
            }
            Object value = variant.getObject();
            if (value == null) {
                return failedRead(
                        itemId,
                        quality,
                        timestamp,
                        ErrorCode.ERROR_CODE_NO_VALUE,
                        "OPC item has no current value",
                        null);
            }
            return ReadResult.success(itemId, value, quality, timestamp);
        } catch (Exception e) {
            log.debug("failed to decode OPC value for '{}': {}", itemId, messageOf(e), e);
            return failedRead(
                    itemId,
                    quality,
                    timestamp,
                    ErrorCode.ERROR_CODE_OPC_FAILURE,
                    "OPC value could not be decoded",
                    null);
        }
    }

    private ReadResult failedRead(
            String itemId,
            int quality,
            long timestamp,
            ErrorCode code,
            String message,
            Integer hresult) {
        return hresult == null
                ? ReadResult.failure(itemId, quality, timestamp, code, message)
                : ReadResult.failure(itemId, quality, timestamp, code, message, hresult);
    }

    private boolean isEmptyVariant(JIVariant variant) throws JIException {
        int type = variant.getType() & ~JIVariant.VT_BYREF;
        return type == JIVariant.VT_EMPTY || type == JIVariant.VT_NULL;
    }

    public Map<String, Integer> write(String connectionId, Map<String, Object> values) {
        return write(connectionId, values, 0);
    }

    /** Writes a batch at most once and never starts the attempt after {@code deadlineMs}. */
    public Map<String, Integer> write(
            String connectionId, Map<String, Object> values, long deadlineMs) {
        if (values.isEmpty()) {
            return Map.of();
        }
        requireRegistered(connectionId);
        ensureBeforeDeadline(deadlineMs);
        rememberItems(connectionId, values.keySet());

        try {
            return writeAttempt(connectionId, values, deadlineMs);
        } catch (DcomExecutor.DcomTimeoutException e) {
            throw e;
        } catch (OpcDaSession.OperationStartTimeoutException e) {
            // This attempt never reached group.write(), but the caller's time budget is exhausted.
            throw e;
        } catch (OpcDaDeadlineException e) {
            throw e;
        } catch (RejectedExecutionException e) {
            // Do not discard a healthy session or amplify overload with a reconnect attempt.
            throw e;
        } catch (Exception error) {
            throw new RuntimeException(
                    "write on '" + connectionId + "' failed: " + messageOf(error), error);
        }
    }

    Map<String, Integer> writeAttempt(
            String connectionId, Map<String, Object> values, long deadlineMs) throws Exception {
        OpcDaSession current = session(connectionId, deadlineMs);
        try {
            return write0(connectionId, current, values, deadlineMs);
        } catch (OpcDaDeadlineException | RejectedExecutionException e) {
            // A timed-out native call can still hold the session operation lock. Keep it cached so
            // another caller cannot open a parallel session while that outcome remains unknown.
            throw e;
        } catch (Exception e) {
            quarantineWriteFailure(connectionId, current);
            throw e;
        }
    }

    private void quarantineWriteFailure(String connectionId, OpcDaSession failedSession) {
        if (!removeIfCurrent(sessions, connectionId, failedSession)) {
            return;
        }
        failedSession.destroy();
        advanceSessionGeneration(connectionId);
        serverTimeCalibration.remove(connectionId);
        log.warn("write failure quarantined session '{}' until the next operation", connectionId);
    }

    static <K, V> boolean removeIfCurrent(ConcurrentMap<K, V> registry, K key, V expected) {
        return registry.remove(key, expected);
    }

    private Map<String, Integer> write0(
            String connectionId, OpcDaSession session, Map<String, Object> values, long deadlineMs)
            throws Exception {
        Map<String, JIVariant> variants = new LinkedHashMap<>();
        values.forEach((id, value) -> variants.put(id, toVariant(value)));
        long startedAt = System.currentTimeMillis();
        long timeoutMs = dcomTimeoutMs(deadlineMs);
        long startBeforeMs = startBefore(timeoutMs);
        Map<String, Integer> results =
                dcom.execute(
                        () -> session.write(variants, startBeforeMs),
                        timeoutMs,
                        "write '" + connectionId + "'");
        lastSuccessByConnection.put(connectionId, System.currentTimeMillis());
        log.debug("write '{}' completed in {}ms", connectionId, elapsedSince(startedAt));
        return results;
    }

    JIVariant toVariant(Object value) {
        return switch (value) {
            case null -> throw new IllegalArgumentException("write value must not be null");
            case JIVariant variant -> variant;
            case Boolean bool -> new JIVariant(bool);
            case Double number -> new JIVariant(number);
            case Float number -> new JIVariant(number);
            case Integer number -> new JIVariant(number);
            case Short number -> new JIVariant(number);
            case Long number -> new JIVariant(number);
            case Unsigned16 number ->
                    new JIVariant(
                            JIUnsignedFactory.getUnsigned(
                                    number.value(), JIFlags.FLAG_REPRESENTATION_UNSIGNED_SHORT));
            case Unsigned32 number ->
                    new JIVariant(
                            JIUnsignedFactory.getUnsigned(
                                    number.value(), JIFlags.FLAG_REPRESENTATION_UNSIGNED_INT));
            case String text -> new JIVariant(text);
            case Instant instant -> new JIVariant(Date.from(instant));
            case Date date -> new JIVariant(date);
            case byte[] bytes -> new JIVariant(new JIArray(boxBytes(bytes)));
            default ->
                    throw new IllegalArgumentException(
                            "unsupported value type: " + value.getClass().getName());
        };
    }

    public boolean isConnected(String connectionId) {
        connectionId = requireConnectionId(connectionId);
        Connection connection = connections.get(connectionId);
        if (connection == null) {
            return false;
        }
        long lastSuccess = lastSuccessByConnection.getOrDefault(connectionId, 0L);
        return isConnected(connectionId, connection, lastSuccess, System.currentTimeMillis());
    }

    public long lastSuccessMs(String connectionId) {
        return lastSuccessByConnection.getOrDefault(requireConnectionId(connectionId), 0L);
    }

    public long sessionGeneration(String connectionId) {
        AtomicLong generation = sessionGenerations.get(connectionId);
        return generation == null ? 0 : generation.get();
    }

    public long getServerTimeMs(String connectionId) {
        return getServerTimeMs(connectionId, 0);
    }

    public long getServerTimeMs(String connectionId, long deadlineMs) {
        long now = System.currentTimeMillis();
        ServerTimeCalibration cached = serverTimeCalibration.get(connectionId);
        if (cached != null && now - cached.measuredAtMs() < SERVER_TIME_RECALIBRATE_MS) {
            return now + cached.offsetMs();
        }
        try {
            OpcDaSession session = session(connectionId, deadlineMs);
            long timeoutMs = dcomTimeoutMs(deadlineMs);
            long startBeforeMs = startBefore(timeoutMs);
            long serverTime =
                    dcom.execute(
                            () -> session.serverTimeMs(startBeforeMs),
                            timeoutMs,
                            "getServerTime '" + connectionId + "'");
            serverTimeCalibration.put(
                    connectionId, new ServerTimeCalibration(serverTime - now, now));
            return serverTime;
        } catch (OpcDaDeadlineException | RejectedExecutionException e) {
            throw e;
        } catch (Exception e) {
            log.debug(
                    "failed to get server time for '{}', using fallback: {}",
                    connectionId,
                    messageOf(e));
            return cached == null ? 0 : now + cached.offsetMs();
        }
    }

    /** Removes a connection and releases all DCOM and cached state for it. */
    private void removeConnection(String connectionId) {
        connectionId = requireConnectionId(connectionId);
        ReentrantLock lock = sessionLock(connectionId);
        lock.lock();
        try {
            if (!connections.containsKey(connectionId)) {
                return;
            }
            retiringConnections.add(connectionId);
            connections.remove(connectionId);
            destroySession(connectionId);
            knownItemsByConnection.remove(connectionId);
            lastSuccessByConnection.remove(connectionId);
            serverTimeCalibration.remove(connectionId);
            sessionGenerations.remove(connectionId);
            log.info("connection '{}' removed", connectionId);
        } finally {
            retiringConnections.remove(connectionId);
            lock.unlock();
        }
    }

    private OpcDaSession session(String connectionId) {
        return session(connectionId, 0);
    }

    private OpcDaSession session(String connectionId, long deadlineMs) {
        ensureBeforeDeadline(deadlineMs);
        requireAvailable(connectionId);
        OpcDaSession existing = sessions.get(connectionId);
        if (existing != null) {
            return existing;
        }

        ReentrantLock lock = sessionLock(connectionId);
        lockSession(lock, connectionId, deadlineMs);
        try {
            ensureBeforeDeadline(deadlineMs);
            requireAvailable(connectionId);
            OpcDaSession current = sessions.get(connectionId);
            if (current != null) {
                return current;
            }
            Connection connection = connections.get(connectionId);
            if (connection == null) {
                throw new IllegalArgumentException("unknown connection: '" + connectionId + "'");
            }
            OpcDaSession created = createSession(connectionId, connection, deadlineMs);
            sessions.put(connectionId, created);
            advanceSessionGeneration(connectionId);
            return created;
        } finally {
            lock.unlock();
        }
    }

    private OpcDaSession reconnectAfterFailure(
            String connectionId, OpcDaSession failedSession, long deadlineMs) {
        ensureBeforeDeadline(deadlineMs);
        ReentrantLock lock = sessionLock(connectionId);
        lockSession(lock, connectionId, deadlineMs);
        try {
            ensureBeforeDeadline(deadlineMs);
            requireAvailable(connectionId);
            OpcDaSession current = sessions.get(connectionId);
            if (current != null && current != failedSession) {
                return current;
            }
            return reconnectLocked(connectionId, deadlineMs);
        } finally {
            lock.unlock();
        }
    }

    private OpcDaSession reconnectLocked(String connectionId, long deadlineMs) {
        ensureBeforeDeadline(deadlineMs);
        destroySession(connectionId);
        // Invalidate state derived from the old connection even if creating its replacement fails.
        advanceSessionGeneration(connectionId);
        serverTimeCalibration.remove(connectionId);
        Connection connection = connections.get(connectionId);
        if (connection == null) {
            throw new IllegalArgumentException("unknown connection: '" + connectionId + "'");
        }

        Set<String> knownItems =
                new LinkedHashSet<>(knownItemsByConnection.getOrDefault(connectionId, Set.of()));
        log.info("reconnecting to '{}' ({} known items)", connectionId, knownItems.size());
        OpcDaSession replacement = createSession(connectionId, connection, deadlineMs);
        sessions.put(connectionId, replacement);
        if (!knownItems.isEmpty()) {
            try {
                long timeoutMs = dcomTimeoutMs(0);
                long startBeforeMs = startBefore(timeoutMs);
                dcom.execute(
                        () -> replacement.ensureItems(knownItems, startBeforeMs),
                        timeoutMs,
                        "restore items '" + connectionId + "'");
            } catch (Exception e) {
                log.warn("failed to restore items on '{}': {}", connectionId, messageOf(e));
            }
        }
        return replacement;
    }

    private OpcDaSession createSession(
            String connectionId, Connection connection, long deadlineMs) {
        String host = connection.getHost();
        Server server = new Server(connectionInformation(connection), dcom.executor());
        server.setDefaultUpdateRate(pollIntervalMillis(connection));
        try {
            OpcDaSession session = dcom.connect(server, host, socketTimeoutMs, deadlineMs);
            lastSuccessByConnection.put(connectionId, System.currentTimeMillis());
            log.info(
                    "connected to '{}' (clsId={}, progId={})",
                    host,
                    connection.getClsId(),
                    connection.getProgId());
            return session;
        } catch (DcomExecutor.AbandonedConnectException e) {
            throw new OpcDaDeadlineException(
                    "failed to connect to '" + host + "': " + messageOf(e), e);
        } catch (OpcDaDeadlineException e) {
            try {
                server.dispose();
            } catch (Exception disposeError) {
                e.addSuppressed(disposeError);
            }
            throw e;
        } catch (RejectedExecutionException e) {
            try {
                server.dispose();
            } catch (Exception disposeError) {
                e.addSuppressed(disposeError);
            }
            throw e;
        } catch (Exception e) {
            try {
                server.dispose();
            } catch (Exception disposeError) {
                e.addSuppressed(disposeError);
            }
            throw new RuntimeException("failed to connect to '" + host + "': " + messageOf(e), e);
        }
    }

    ConnectionInformation connectionInformation(Connection connection) {
        ConnectionInformation connectionInfo = new ConnectionInformation();
        connectionInfo.setHost(connection.getHost());
        connectionInfo.setDomain(connection.getDomain());
        connectionInfo.setUser(connection.getUsername());
        connectionInfo.setPassword(connection.getCredentials().getPassword());
        if (!connection.getClsId().isBlank()) {
            connectionInfo.setClsid(connection.getClsId().replace("{", "").replace("}", ""));
        } else if (!connection.getProgId().isBlank()) {
            connectionInfo.setProgId(connection.getProgId());
        } else {
            throw new IllegalArgumentException("server must define cls_id or prog_id");
        }
        return connectionInfo;
    }

    private long dcomTimeoutMs(long deadlineMs) {
        if (deadlineMs <= 0) {
            return socketTimeoutMs;
        }
        long remainingMs = deadlineMs - System.currentTimeMillis();
        if (remainingMs <= 0) {
            throw new OpcDaDeadlineException("operation deadline exceeded before DCOM call");
        }
        return Math.min(socketTimeoutMs, remainingMs);
    }

    private void lockSession(ReentrantLock lock, String connectionId, long deadlineMs) {
        if (deadlineMs <= 0) {
            lock.lock();
            return;
        }
        long remainingMs = deadlineMs - System.currentTimeMillis();
        try {
            if (remainingMs <= 0
                    || !lock.tryLock(remainingMs, java.util.concurrent.TimeUnit.MILLISECONDS)) {
                throw new OpcDaDeadlineException(
                        "deadline exceeded while waiting for session '" + connectionId + "'");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OpcDaDeadlineException(
                    "interrupted while waiting for session '" + connectionId + "'", e);
        }
    }

    private long startBefore(long timeoutMs) {
        return Math.addExact(System.currentTimeMillis(), timeoutMs);
    }

    private void ensureBeforeDeadline(long deadlineMs) {
        if (deadlineMs > 0 && System.currentTimeMillis() >= deadlineMs) {
            throw new OpcDaDeadlineException("operation deadline exceeded before processing");
        }
    }

    private void requireAvailable(String connectionId) {
        if (retiringConnections.contains(connectionId)) {
            throw new IllegalStateException("connection '" + connectionId + "' is being replaced");
        }
    }

    private boolean isConnected(
            String connectionId, Connection connection, long lastSuccess, long now) {
        if (retiringConnections.contains(connectionId)
                || !sessions.containsKey(connectionId)
                || lastSuccess == 0) {
            return false;
        }
        long thresholdMs = Math.min(60_000, Math.max(10_000, pollIntervalMillis(connection) * 2L));
        return now - lastSuccess <= thresholdMs;
    }

    private String requireConnectionId(String connectionId) {
        if (connectionId == null || connectionId.isBlank()) {
            throw new IllegalArgumentException("connection ID is required");
        }
        return connectionId;
    }

    private String requireHost(String host) {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("server host is required");
        }
        return host;
    }

    private void requireRegistered(String connectionId) {
        if (!hasConnection(connectionId)) {
            throw new IllegalArgumentException("unknown connection: '" + connectionId + "'");
        }
    }

    private void validateConnection(Connection connection) {
        requireConnectionId(connection.getId());
        requireHost(connection.getHost());
        switch (connection.getServerIdentifierCase()) {
            case CLS_ID -> {
                if (connection.getClsId().isBlank()) {
                    throw new IllegalArgumentException("cls_id must not be blank");
                }
            }
            case PROG_ID -> {
                if (connection.getProgId().isBlank()) {
                    throw new IllegalArgumentException("prog_id must not be blank");
                }
            }
            case SERVERIDENTIFIER_NOT_SET ->
                    throw new IllegalArgumentException("cls_id or prog_id is required");
        }
        pollIntervalMillis(connection);
    }

    private int pollIntervalMillis(Connection connection) {
        long intervalMs = connection.getPollIntervalMs();
        if (intervalMs < 1 || intervalMs > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "poll_interval_ms must be between 1 and " + Integer.MAX_VALUE);
        }
        return (int) intervalMs;
    }

    private ReentrantLock sessionLock(String connectionId) {
        int index =
                Math.floorMod(requireConnectionId(connectionId).hashCode(), sessionLocks.length);
        return sessionLocks[index];
    }

    private static ReentrantLock[] createSessionLocks() {
        ReentrantLock[] locks = new ReentrantLock[SESSION_LOCK_STRIPES];
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new ReentrantLock(true);
        }
        return locks;
    }

    private void destroySession(String connectionId) {
        OpcDaSession removed = sessions.remove(connectionId);
        if (removed != null) {
            removed.destroy();
        }
    }

    private void advanceSessionGeneration(String connectionId) {
        sessionGenerations
                .computeIfAbsent(connectionId, ignored -> new AtomicLong())
                .incrementAndGet();
    }

    private void rememberItems(String connectionId, Collection<String> itemIds) {
        knownItemsByConnection
                .computeIfAbsent(connectionId, ignored -> ConcurrentHashMap.newKeySet())
                .addAll(itemIds);
    }

    private Byte[] boxBytes(byte[] bytes) {
        Byte[] boxed = new Byte[bytes.length];
        for (int index = 0; index < bytes.length; index++) {
            boxed[index] = bytes[index];
        }
        return boxed;
    }

    private long elapsedSince(long startedAt) {
        return System.currentTimeMillis() - startedAt;
    }

    private String messageOf(Throwable error) {
        return error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage();
    }

    @PreDestroy
    void shutdown() {
        retiringConnections.addAll(connections.keySet());
        sessions.values().forEach(OpcDaSession::destroy);
        sessions.clear();
        connections.clear();
        knownItemsByConnection.clear();
        lastSuccessByConnection.clear();
        serverTimeCalibration.clear();
        sessionGenerations.clear();
        retiringConnections.clear();
        dcom.shutdown();
        log.info("OpcDaClient shut down");
    }
}
