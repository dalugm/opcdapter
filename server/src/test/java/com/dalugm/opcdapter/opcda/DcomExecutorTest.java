/* (C) 2026 */
package com.dalugm.opcdapter.opcda;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.openscada.opc.lib.common.ConnectionInformation;
import org.openscada.opc.lib.da.Server;

class DcomExecutorTest {

    private static final class TestServer extends Server {
        private final AtomicInteger connectCalls = new AtomicInteger();
        private final AtomicInteger disposeCalls = new AtomicInteger();

        TestServer(ScheduledExecutorService executor) {
            super(new ConnectionInformation(), executor);
        }

        @Override
        public synchronized void connect() {
            connectCalls.incrementAndGet();
        }

        @Override
        public void dispose() {
            disposeCalls.incrementAndGet();
        }
    }

    @Test
    void timedOutNativeCallRetainsAdmissionUntilItActuallyExits() throws Exception {
        DcomExecutor dcom = new DcomExecutor(1, 0);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch exited = new CountDownLatch(1);
        try {
            assertThrows(
                    DcomExecutor.DcomTimeoutException.class,
                    () ->
                            dcom.execute(
                                    () -> {
                                        try {
                                            release.await();
                                            return 1;
                                        } finally {
                                            exited.countDown();
                                        }
                                    },
                                    25,
                                    "blocked read"));

            assertThrows(
                    RejectedExecutionException.class,
                    () -> dcom.execute(() -> 2, 100, "second read"));

            release.countDown();
            assertTrue(exited.await(2, TimeUnit.SECONDS));
            assertEquals(3, dcom.execute(() -> 3, 100, "recovered read"));
        } finally {
            release.countDown();
            dcom.shutdown();
        }
    }

    @Test
    void queuedConnectTimeoutCancelsBeforeStartAndDisposesServer() throws Exception {
        DcomExecutor dcom = new DcomExecutor(1, 1);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Throwable> blockerFailure = new AtomicReference<>();
        Thread blocker =
                Thread.startVirtualThread(
                        () -> {
                            try {
                                dcom.execute(
                                        () -> {
                                            entered.countDown();
                                            release.await();
                                            return null;
                                        },
                                        2_000,
                                        "blocking operation");
                            } catch (Throwable error) {
                                blockerFailure.set(error);
                            }
                        });
        try {
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            TestServer server = new TestServer(dcom.executor());

            assertThrows(
                    DcomExecutor.AbandonedConnectException.class,
                    () ->
                            dcom.connect(
                                    server, "queued-host", 5_000, System.currentTimeMillis() + 25));

            assertEquals(0, server.connectCalls.get());
            assertEquals(1, server.disposeCalls.get());
        } finally {
            release.countDown();
            blocker.join(TimeUnit.SECONDS.toMillis(2));
            dcom.shutdown();
        }
        assertNull(blockerFailure.get());
    }
}
