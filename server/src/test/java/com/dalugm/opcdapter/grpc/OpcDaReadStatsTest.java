/* (C) 2026 */
package com.dalugm.opcdapter.grpc;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import io.grpc.stub.StreamObserver;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.dalugm.opcdapter.api.opcda.v1.*;
import com.dalugm.opcdapter.opcda.OpcDaClient;
import com.dalugm.opcdapter.opcda.ReadResult;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

class OpcDaReadStatsTest {
    @Test
    void unaryReadsAreReportedAndDrainedWithoutPollOrWrites() throws Exception {
        assertSummary(
                List.of(ReadResult.success("A", 42.0, 0xC0, 123)),
                false,
                "read: 1 call(s), 1 value(s), 0 failed call(s), 0 item error(s), 0 non-good"
                        + " quality");
    }

    @Test
    void itemErrorsAndQualityAreNotCountedAsConnectionFailures() throws Exception {
        assertSummary(
                List.of(
                        ReadResult.success("A", 42.0, 0xC0, 123),
                        ReadResult.success("B", 0.0, 0, 123),
                        ReadResult.failure(
                                "C", 0, 123, ErrorCode.ERROR_CODE_OPC_FAILURE, "item failed")),
                false,
                "read: 1 call(s), 3 value(s), 0 failed call(s), 1 item error(s), 1 non-good"
                        + " quality");
    }

    @Test
    void failedReadBatchStillProducesSummary() throws Exception {
        assertSummary(
                List.of(),
                true,
                "read: 1 call(s), 0 value(s), 1 failed call(s), 0 item error(s), 0 non-good"
                        + " quality");
    }

    private void assertSummary(List<ReadResult> values, boolean fail, String expected)
            throws Exception {
        OpcDaClient client =
                new OpcDaClient(1) {
                    @Override
                    public boolean hasConnection(String id) {
                        return true;
                    }

                    @Override
                    public String host(String id) {
                        return "test-host";
                    }

                    @Override
                    public long getServerTimeMs(String id, long deadline) {
                        return 123;
                    }

                    @Override
                    public List<ReadResult> read(
                            String id, List<String> items, boolean device, long deadline) {
                        if (fail) throw new IllegalStateException("DCOM unavailable");
                        return values;
                    }
                };
        OpcDaServiceImpl service = new OpcDaServiceImpl(client, false);
        Logger logger = (Logger) LoggerFactory.getLogger(OpcDaServiceImpl.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            service.read(
                    ReadRequest.newBuilder()
                            .setRequestId("stats-read")
                            .setConnectionId("connection-1")
                            .addAllItemIds(
                                    values.isEmpty()
                                            ? List.of("A")
                                            : values.stream().map(ReadResult::itemId).toList())
                            .setSource(ReadSource.READ_SOURCE_DEVICE)
                            .build(),
                    new StreamObserver<>() {
                        @Override
                        public void onNext(ReadResponse response) {
                            assertEquals(fail, response.getBatch().hasError());
                        }

                        @Override
                        public void onError(Throwable error) {
                            fail(error);
                        }

                        @Override
                        public void onCompleted() {}
                    });
            // Trigger the existing periodic callback deterministically, without a 60s sleep.
            var logStats = OpcDaServiceImpl.class.getDeclaredMethod("logStats");
            logStats.setAccessible(true);
            logStats.invoke(service);
            var summaries =
                    appender.list.stream()
                            .map(ILoggingEvent::getFormattedMessage)
                            .filter(message -> message.startsWith("stats (last 60s)"))
                            .toList();
            assertEquals(1, summaries.size(), "Read-only traffic must not be treated as idle");
            assertTrue(summaries.getFirst().contains(expected), summaries.toString());
            appender.list.clear();
            logStats.invoke(service);
            assertTrue(appender.list.isEmpty(), "An idle interval must not repeat old counters");
        } finally {
            logger.detachAppender(appender);
            appender.stop();
            service.shutdown();
            var shutdown = OpcDaClient.class.getDeclaredMethod("shutdown");
            shutdown.setAccessible(true);
            shutdown.invoke(client);
        }
    }
}
