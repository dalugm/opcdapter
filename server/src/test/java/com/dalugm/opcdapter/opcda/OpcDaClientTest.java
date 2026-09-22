/* (C) 2026 */
package com.dalugm.opcdapter.opcda;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.jinterop.dcom.core.JIVariant;
import org.junit.jupiter.api.Test;
import org.openscada.opc.lib.common.ConnectionInformation;
import org.openscada.opc.lib.da.ItemState;

import com.dalugm.opcdapter.api.opcda.v1.Connection;
import com.dalugm.opcdapter.api.opcda.v1.ConnectionCredentials;
import com.dalugm.opcdapter.api.opcda.v1.ErrorCode;

class OpcDaClientTest {

    @Test
    void usesProgIdWhenClsidIsAbsent() {
        OpcDaClient client = new OpcDaClient(1);
        try {
            ConnectionInformation connection =
                    client.connectionInformation(
                            Connection.newBuilder()
                                    .setId("connection-1")
                                    .setHost("opc.example")
                                    .setProgId("Vendor.OPC.Server")
                                    .setCredentials(ConnectionCredentials.getDefaultInstance())
                                    .setPollIntervalMs(1_000)
                                    .build());

            assertNull(connection.getClsid());
            assertEquals("Vendor.OPC.Server", connection.getProgId());
        } finally {
            client.shutdown();
        }
    }

    @Test
    void configuresTheSocketTimeoutPropertyUsedByUtgard() {
        OpcDaClient client = new OpcDaClient(1);
        try {
            client.init();
            assertEquals("5000", System.getProperty("rpc.socketTimeout"));
        } finally {
            client.shutdown();
        }
    }

    @Test
    void supportsDateAndByteArrayVariants() throws Exception {
        OpcDaClient client = new OpcDaClient(1);
        try {
            assertEquals(JIVariant.VT_DATE, client.toVariant(new Date()).getType());
            assertEquals(JIVariant.VT_DATE, client.toVariant(Instant.now()).getType());
            assertNotEquals(
                    0, client.toVariant(new byte[] {1, 2, 3}).getType() & JIVariant.VT_ARRAY);
        } finally {
            client.shutdown();
        }
    }

    @Test
    void treatsEmptyVariantAsNoCurrentValue() {
        OpcDaClient client = new OpcDaClient(1);
        try {
            Calendar timestamp = Calendar.getInstance();
            ItemState state = new ItemState(0, JIVariant.EMPTY(), timestamp, (short) 0);

            ReadResult result = client.toReadResult("Point1", state);

            assertNull(result.value());
            assertEquals("OPC item has no current value", result.error());
            assertEquals(ErrorCode.ERROR_CODE_NO_VALUE, result.errorCode());
            assertEquals(timestamp.getTimeInMillis(), result.sourceTimestampMs());
            assertEquals(0, result.quality());
        } finally {
            client.shutdown();
        }
    }

    @Test
    void preservesPerItemReadErrorBeforeInspectingValue() {
        OpcDaClient client = new OpcDaClient(1);
        try {
            ItemState state =
                    new ItemState(
                            OpcDaHresult.UNKNOWN_ITEM_ID.value(),
                            JIVariant.EMPTY(),
                            null,
                            (short) 0);

            ReadResult result = client.toReadResult("MissingPoint", state);

            assertNull(result.value());
            assertEquals("OPC item not found", result.error());
            assertEquals(ErrorCode.ERROR_CODE_ITEM_NOT_FOUND, result.errorCode());
            assertEquals(OpcDaHresult.UNKNOWN_ITEM_ID.value(), result.hresult().orElseThrow());
        } finally {
            client.shutdown();
        }
    }

    @Test
    void setConnectionsIsIdempotentAndDoesNotDial() {
        OpcDaClient client = new OpcDaClient(1);
        try {
            Connection connection = connection("connection-1", "opc.example");

            assertEquals(Set.of("connection-1"), client.setConnections(List.of(connection)));
            assertEquals(List.of("connection-1"), client.connectionIds());
            assertFalse(client.isConnected("connection-1"));
            assertEquals(
                    List.of(
                            new OpcDaClient.ConnectionHealth(
                                    "connection-1", "opc.example", false, 0)),
                    client.connectionHealth());
            assertTrue(client.setConnections(List.of(connection)).isEmpty());
        } finally {
            client.shutdown();
        }
    }

    @Test
    void setConnectionsRejectsInvalidReplacementWithoutChangingRegistry() {
        OpcDaClient client = new OpcDaClient(1);
        try {
            client.setConnections(List.of(connection("connection-1", "opc.example")));
            Connection invalid =
                    connection("connection-2", "opc.invalid").toBuilder()
                            .setPollIntervalMs(0)
                            .build();

            assertThrows(
                    IllegalArgumentException.class, () -> client.setConnections(List.of(invalid)));
            assertEquals(List.of("connection-1"), client.connectionIds());
        } finally {
            client.shutdown();
        }
    }

    @Test
    void setConnectionsRejectsNegativePollIntervalWithoutChangingRegistry() {
        OpcDaClient client = new OpcDaClient(1);
        try {
            client.setConnections(List.of(connection("connection-1", "opc.example")));
            Connection invalid =
                    connection("connection-2", "opc.invalid").toBuilder()
                            .setPollIntervalMs(-1)
                            .build();

            assertThrows(
                    IllegalArgumentException.class, () -> client.setConnections(List.of(invalid)));
            assertEquals(List.of("connection-1"), client.connectionIds());
        } finally {
            client.shutdown();
        }
    }

    @Test
    void setConnectionsRemovesOmittedConnections() {
        OpcDaClient client = new OpcDaClient(1);
        try {
            client.setConnections(
                    List.of(
                            connection("connection-1", "opc-1.example"),
                            connection("connection-2", "opc-2.example")));

            assertEquals(
                    Set.of("connection-1"),
                    client.setConnections(List.of(connection("connection-2", "opc-2.example"))));
            assertEquals(List.of("connection-2"), client.connectionIds());
        } finally {
            client.shutdown();
        }
    }

    @Test
    void writeFailureIsNotRetried() {
        NoRetryWriteClient client = new NoRetryWriteClient();
        try {
            client.setConnections(List.of(connection("connection-1", "opc.example")));

            assertThrows(
                    RuntimeException.class,
                    () -> client.write("connection-1", Map.of("Point1", 1), 0));
            assertEquals(1, client.attempts.get(), "an uncertain write must not be retried");
        } finally {
            client.shutdown();
        }
    }

    @Test
    void connectionWideReadFailureEscapesTheItemResultBoundary() {
        ConnectionFailureReadClient client = new ConnectionFailureReadClient();
        try {
            client.setConnections(List.of(connection("connection-1", "opc.example")));

            OpcDaClient.ReadOperationException failure =
                    assertThrows(
                            OpcDaClient.ReadOperationException.class,
                            () -> client.read("connection-1", List.of("Point1"), true, 0));

            assertEquals(ErrorCode.ERROR_CODE_OPC_FAILURE, failure.errorCode());
        } finally {
            client.shutdown();
        }
    }

    @Test
    void laterWriteCanRecoverWithoutReplayingFailedWrite() {
        RecoveringWriteClient client = new RecoveringWriteClient();
        try {
            client.setConnections(List.of(connection("connection-1", "opc.example")));

            assertThrows(
                    RuntimeException.class,
                    () -> client.write("connection-1", Map.of("Point1", 1), 0));
            assertEquals(1, client.attempts.get());

            assertEquals(Map.of("Point2", 0), client.write("connection-1", Map.of("Point2", 2), 0));
            assertEquals(2, client.attempts.get());
        } finally {
            client.shutdown();
        }
    }

    @Test
    void quarantineRemovalCannotRemoveAReplacementSession() {
        var registry = new ConcurrentHashMap<String, Object>();
        Object failed = new Object();
        Object replacement = new Object();
        registry.put("connection-1", replacement);

        assertFalse(OpcDaClient.removeIfCurrent(registry, "connection-1", failed));
        assertSame(replacement, registry.get("connection-1"));

        registry.put("connection-1", failed);
        assertTrue(OpcDaClient.removeIfCurrent(registry, "connection-1", failed));
        assertFalse(registry.containsKey("connection-1"));
    }

    private static final class NoRetryWriteClient extends OpcDaClient {
        final AtomicInteger attempts = new AtomicInteger();

        NoRetryWriteClient() {
            super(1);
        }

        @Override
        Map<String, Integer> writeAttempt(
                String connectionId, Map<String, Object> values, long deadlineMs) {
            attempts.incrementAndGet();
            throw new IllegalStateException("uncertain failure");
        }
    }

    private static final class ConnectionFailureReadClient extends OpcDaClient {
        ConnectionFailureReadClient() {
            super(1);
        }

        @Override
        List<ReadResult> readAttempts(
                String connectionId, List<String> itemIds, boolean device, long deadlineMs) {
            throw new IllegalStateException("connection failed");
        }
    }

    private static final class RecoveringWriteClient extends OpcDaClient {
        final AtomicInteger attempts = new AtomicInteger();

        RecoveringWriteClient() {
            super(1);
        }

        @Override
        Map<String, Integer> writeAttempt(
                String connectionId, Map<String, Object> values, long deadlineMs) {
            if (attempts.incrementAndGet() == 1) {
                throw new IllegalStateException("uncertain failure");
            }
            return values.keySet().stream()
                    .collect(java.util.stream.Collectors.toMap(id -> id, ignored -> 0));
        }
    }

    private static Connection connection(String id, String host) {
        return Connection.newBuilder()
                .setId(id)
                .setHost(host)
                .setProgId("Vendor.OPC.Server")
                .setPollIntervalMs(1_000)
                .build();
    }
}
