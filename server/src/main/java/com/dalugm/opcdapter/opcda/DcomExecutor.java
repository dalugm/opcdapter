/* (C) 2026 */
package com.dalugm.opcdapter.opcda;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.openscada.opc.lib.da.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Executes blocking Utgard calls on bounded platform threads without interrupting DCOM calls. */
final class DcomExecutor {

    static final class DcomTimeoutException extends OpcDaDeadlineException {
        DcomTimeoutException(String operation, long timeoutMs) {
            super(operation + " timed out after " + timeoutMs + "ms");
        }
    }

    static final class AbandonedConnectException extends OpcDaDeadlineException {
        AbandonedConnectException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(DcomExecutor.class);
    private static final long CONNECT_TIMEOUT_MS = 5_000;

    private final ScheduledThreadPoolExecutor executor;
    private final Semaphore admission;

    DcomExecutor(int threads, int queueCapacity) {
        if (threads <= 0 || queueCapacity < 0) {
            throw new IllegalArgumentException(
                    "threads must be > 0 and queueCapacity must be >= 0");
        }
        executor =
                new ScheduledThreadPoolExecutor(
                        threads, Thread.ofPlatform().daemon().name("utgard-exec-", 0).factory());
        executor.setRemoveOnCancelPolicy(true);
        admission = new Semaphore(Math.addExact(threads, queueCapacity), true);
    }

    ScheduledExecutorService executor() {
        return executor;
    }

    OpcDaSession connect(Server server, String host, int socketTimeoutMs, long deadlineMs)
            throws Exception {
        long timeoutMs = connectTimeoutMs(socketTimeoutMs, deadlineMs);
        ConnectAttempt attempt = new ConnectAttempt(server, host);
        Future<OpcDaSession> future = submit(attempt, "connect '" + host + "'");
        try {
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            attempt.abandon(future);
            throw new AbandonedConnectException(
                    "connect '" + host + "' timed out after " + timeoutMs + "ms", e);
        } catch (ExecutionException e) {
            throw unwrap(e);
        } catch (InterruptedException e) {
            attempt.abandon(future);
            Thread.currentThread().interrupt();
            throw new AbandonedConnectException("connect '" + host + "' was interrupted", e);
        }
    }

    private long connectTimeoutMs(int socketTimeoutMs, long deadlineMs) {
        long timeoutMs = Math.max(CONNECT_TIMEOUT_MS, (long) socketTimeoutMs + 1_000);
        if (deadlineMs <= 0) {
            return timeoutMs;
        }
        long remainingMs = deadlineMs - System.currentTimeMillis();
        if (remainingMs <= 0) {
            throw new OpcDaDeadlineException("deadline exceeded before DCOM connect");
        }
        return Math.min(timeoutMs, remainingMs);
    }

    private void disposeAbandonedServer(Server server, String host) {
        try {
            server.dispose();
        } catch (Exception e) {
            log.debug("abandoned connection '{}' cleanup failed: {}", host, messageOf(e));
        }
    }

    <T> T execute(Callable<T> operation, long timeoutMs, String description) throws Exception {
        Future<T> future = submit(operation, description);
        try {
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(false);
            throw new DcomTimeoutException(description, timeoutMs);
        } catch (ExecutionException e) {
            throw unwrap(e);
        } catch (InterruptedException e) {
            future.cancel(false);
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    private <T> Future<T> submit(Callable<T> operation, String description) {
        if (!admission.tryAcquire()) {
            log.warn("DCOM executor overloaded; rejecting {}", description);
            throw new RejectedExecutionException("DCOM executor capacity exhausted");
        }
        BoundedFutureTask<T> task = new BoundedFutureTask<>(operation);
        try {
            executor.execute(task);
            return task;
        } catch (RuntimeException e) {
            task.cancel(false);
            throw e;
        }
    }

    private final class ConnectAttempt implements Callable<OpcDaSession> {
        private final Server server;
        private final String host;
        private final Object lifecycleLock = new Object();

        private boolean started;
        private boolean finished;
        private boolean abandoned;
        private boolean cleanupClaimed;

        ConnectAttempt(Server server, String host) {
            this.server = server;
            this.host = host;
        }

        @Override
        public OpcDaSession call() throws Exception {
            synchronized (lifecycleLock) {
                if (abandoned) {
                    finished = true;
                    throw new OpcDaDeadlineException(
                            "connect '" + host + "' was abandoned before starting");
                }
                started = true;
            }
            try {
                server.connect();
                return new OpcDaSession(host, server);
            } finally {
                boolean cleanup;
                synchronized (lifecycleLock) {
                    finished = true;
                    cleanup = abandoned && !cleanupClaimed;
                    cleanupClaimed |= cleanup;
                }
                if (cleanup) {
                    disposeAbandonedServer(server, host);
                }
            }
        }

        void abandon(Future<?> future) {
            boolean cleanup;
            synchronized (lifecycleLock) {
                abandoned = true;
                cleanup = (!started || finished) && !cleanupClaimed;
                cleanupClaimed |= cleanup;
            }
            // Interrupting native DCOM calls can leave their state inconsistent.
            future.cancel(false);
            if (cleanup) {
                disposeAbandonedServer(server, host);
            }
        }
    }

    private final class BoundedFutureTask<T> extends FutureTask<T> {
        private final AtomicBoolean started = new AtomicBoolean();
        private final AtomicBoolean released = new AtomicBoolean();

        BoundedFutureTask(Callable<T> operation) {
            super(operation);
        }

        @Override
        public void run() {
            started.set(true);
            try {
                super.run();
            } finally {
                releaseAdmission();
            }
        }

        @Override
        protected void done() {
            if (!started.get()) {
                releaseAdmission();
            }
        }

        private void releaseAdmission() {
            if (released.compareAndSet(false, true)) {
                admission.release();
            }
        }
    }

    private Exception unwrap(ExecutionException error) {
        Throwable cause = error.getCause() == null ? error : error.getCause();
        if (cause instanceof Error fatal) {
            throw fatal;
        }
        if (cause instanceof Exception exception) {
            return exception;
        }
        return new RuntimeException(cause);
    }

    void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private String messageOf(Throwable error) {
        return error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage();
    }
}
