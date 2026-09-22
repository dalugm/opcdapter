/* (C) 2026 */
package com.dalugm.opcdapter.opcda;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.jinterop.dcom.core.JIVariant;
import org.openscada.opc.lib.da.AddFailedException;
import org.openscada.opc.lib.da.Group;
import org.openscada.opc.lib.da.Item;
import org.openscada.opc.lib.da.ItemState;
import org.openscada.opc.lib.da.Server;
import org.openscada.opc.lib.da.WriteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One OPC server connection and group. All Utgard access is serialized by {@code operationLock}.
 */
final class OpcDaSession {
    private static final Logger log = LoggerFactory.getLogger(OpcDaSession.class);

    static final class OperationStartTimeoutException extends OpcDaDeadlineException {
        OperationStartTimeoutException(String message) {
            super(message);
        }
    }

    private final String host;
    private final Server server;
    private final Group group;
    private final Map<String, Item> items = new LinkedHashMap<>();
    private final ReentrantLock operationLock = new ReentrantLock(true);
    private final AtomicBoolean cleanupScheduled = new AtomicBoolean();

    private volatile boolean retired;
    private boolean disposed;

    OpcDaSession(String host, Server server) {
        this.host = host;
        this.server = server;
        try {
            group = server.addGroup("session-" + host);
        } catch (Exception e) {
            throw new RuntimeException("failed to create group for '" + host + "': " + e, e);
        }
    }

    Set<String> ensureItems(Collection<String> itemIds, long startBeforeMs) {
        lockForOperation(startBeforeMs);
        try {
            ensureUsable();
            return Set.copyOf(ensureItemsLocked(itemIds).keySet());
        } finally {
            operationLock.unlock();
        }
    }

    Map<String, ItemState> read(Collection<String> itemIds, boolean device, long startBeforeMs)
            throws Exception {
        lockForOperation(startBeforeMs);
        try {
            ensureUsable();
            Map<String, Item> requested = ensureItemsLocked(itemIds);
            if (requested.isEmpty()) {
                return Map.of();
            }

            ensureCanStart(startBeforeMs, "read");
            Map<Item, ItemState> states =
                    group.read(device, requested.values().toArray(Item[]::new));
            Map<String, ItemState> byId = new LinkedHashMap<>();
            requested.forEach((id, item) -> byId.put(id, states.get(item)));
            return byId;
        } finally {
            operationLock.unlock();
        }
    }

    Map<String, Integer> write(Map<String, JIVariant> values, long startBeforeMs) throws Exception {
        lockForOperation(startBeforeMs);
        try {
            ensureUsable();
            Map<String, Item> requested = ensureItemsLocked(values.keySet());
            Map<String, Integer> results = new LinkedHashMap<>();
            values.keySet().stream()
                    .filter(id -> !requested.containsKey(id))
                    .forEach(id -> results.put(id, -1));
            if (requested.isEmpty()) {
                return results;
            }

            List<WriteRequest> requests = new ArrayList<>(requested.size());
            requested.forEach((id, item) -> requests.add(new WriteRequest(item, values.get(id))));
            ensureCanStart(startBeforeMs, "write");
            Map<Item, Integer> writeResults = group.write(requests.toArray(WriteRequest[]::new));
            requested.forEach((id, item) -> results.put(id, writeResults.getOrDefault(item, -1)));
            return results;
        } finally {
            operationLock.unlock();
        }
    }

    long serverTimeMs(long startBeforeMs) throws Exception {
        lockForOperation(startBeforeMs);
        try {
            ensureUsable();
            var status = server.getServerState();
            if (status == null || status.getCurrentTime() == null) {
                throw new IllegalStateException("OPC server returned no current time");
            }
            return status.getCurrentTime().asCalendar().getTimeInMillis();
        } finally {
            operationLock.unlock();
        }
    }

    void destroy() {
        retired = true;
        if (operationLock.tryLock()) {
            try {
                disposeLocked();
            } finally {
                operationLock.unlock();
            }
            return;
        }

        // A timed-out native DCOM call may keep running after its Future stops
        // being observed. Quarantine the session immediately and let a daemon
        // virtual thread dispose it once that call releases the operation lock.
        if (cleanupScheduled.compareAndSet(false, true)) {
            log.warn("session '{}' cleanup deferred until the active DCOM call exits", host);
            Thread.startVirtualThread(this::destroyWhenIdle);
        }
    }

    private void destroyWhenIdle() {
        operationLock.lock();
        try {
            disposeLocked();
        } finally {
            operationLock.unlock();
        }
    }

    private void disposeLocked() {
        if (disposed) {
            return;
        }
        disposed = true;
        try {
            server.removeGroup(group, true);
        } catch (Exception e) {
            log.debug("session '{}' removeGroup failed: {}", host, e.getMessage());
        }
        items.clear();
        try {
            server.dispose();
        } catch (Exception e) {
            log.debug("session '{}' server.dispose failed: {}", host, e.getMessage());
        }
    }

    private void lockForOperation(long startBeforeMs) {
        try {
            long remainingMs = startBeforeMs - System.currentTimeMillis();
            if (remainingMs <= 0 || !operationLock.tryLock(remainingMs, TimeUnit.MILLISECONDS)) {
                throw new OperationStartTimeoutException(
                        "operation deadline exceeded before acquiring session lock for '"
                                + host
                                + "'");
            }
            try {
                ensureCanStart(startBeforeMs, "DCOM operation");
            } catch (RuntimeException e) {
                operationLock.unlock();
                throw e;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "interrupted while waiting for session lock for '" + host + "'", e);
        }
    }

    private void ensureCanStart(long startBeforeMs, String operation) {
        if (System.currentTimeMillis() >= startBeforeMs) {
            throw new OperationStartTimeoutException(
                    operation + " deadline exceeded before starting DCOM call for '" + host + "'");
        }
    }

    private Map<String, Item> ensureItemsLocked(Collection<String> itemIds) {
        List<String> missing = itemIds.stream().filter(id -> !items.containsKey(id)).toList();
        if (!missing.isEmpty()) {
            Map<String, Item> added;
            try {
                added = group.addItems(missing.toArray(String[]::new));
            } catch (AddFailedException e) {
                added = e.getItems();
            } catch (Exception e) {
                log.warn("session '{}' addItems failed: {}", host, e.getMessage());
                added = Map.of();
            }
            items.putAll(added);
        }

        Map<String, Item> requested = new LinkedHashMap<>();
        itemIds.forEach(
                id -> {
                    Item item = items.get(id);
                    if (item != null) {
                        requested.put(id, item);
                    }
                });
        return requested;
    }

    private void ensureUsable() {
        if (retired) {
            throw new IllegalStateException("session for '" + host + "' has been destroyed");
        }
    }
}
