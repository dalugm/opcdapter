/* (C) 2026 */
package com.dalugm.opcdapter.grpc;

import static org.junit.jupiter.api.Assertions.*;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;

import org.junit.jupiter.api.*;

import com.dalugm.opcdapter.api.opcda.v1.*;
import com.dalugm.opcdapter.api.opcda.v1.OpcDaServiceGrpc.OpcDaServiceBlockingStub;
import com.dalugm.opcdapter.api.opcda.v1.OpcDaServiceGrpc.OpcDaServiceStub;
import com.dalugm.opcdapter.opcda.OpcDaClient;
import com.dalugm.opcdapter.opcda.OpcDaHresult;
import com.dalugm.opcdapter.opcda.ReadResult;

class OpcDaServiceImplTest {

    private static OpcDaServiceImpl service;
    private static io.grpc.Server server;
    private static ManagedChannel channel;

    private static final AtomicInteger readCount = new AtomicInteger();
    private static final AtomicInteger writeCount = new AtomicInteger();

    private static final OpcDaClient fakeClient =
            new OpcDaClient(16) {
                @Override
                public boolean hasConnection(String connectionId) {
                    return true;
                }

                @Override
                public int pollIntervalMs(String connectionId) {
                    return 50;
                }

                @Override
                public Set<String> validateItems(
                        String connectionId, java.util.Collection<String> itemIds) {
                    return new LinkedHashSet<>(itemIds);
                }

                @Override
                public List<ConnectionHealth> connectionHealth() {
                    return List.of(
                            new ConnectionHealth(
                                    "connection-1", "10.0.0.1", true, System.currentTimeMillis()));
                }

                @Override
                public String host(String connectionId) {
                    return "10.0.0.1";
                }

                @Override
                public long getServerTimeMs(String connectionId) {
                    return System.currentTimeMillis();
                }

                @Override
                public long getServerTimeMs(String connectionId, long deadlineMs) {
                    return getServerTimeMs(connectionId);
                }

                @Override
                public List<ReadResult> read(String connectionId, List<String> itemIds) {
                    return read(connectionId, itemIds, false, 0);
                }

                @Override
                public List<ReadResult> read(
                        String connectionId,
                        List<String> itemIds,
                        boolean device,
                        long deadlineMs) {
                    readCount.incrementAndGet();
                    return itemIds.stream()
                            .map(
                                    id ->
                                            ReadResult.success(
                                                    id, 42.0, 0xC0, System.currentTimeMillis()))
                            .toList();
                }

                @Override
                public java.util.Map<String, Integer> write(
                        String connectionId, java.util.Map<String, Object> values) {
                    writeCount.incrementAndGet();
                    return values.keySet().stream()
                            .collect(
                                    java.util.stream.Collectors.toMap(
                                            id -> id,
                                            id -> 0,
                                            (a, b) -> a,
                                            java.util.LinkedHashMap::new));
                }

                @Override
                public java.util.Map<String, Integer> write(
                        String connectionId,
                        java.util.Map<String, Object> values,
                        long deadlineMs) {
                    return write(connectionId, values);
                }
            };

    @BeforeAll
    static void setup() throws Exception {
        String name = InProcessServerBuilder.generateName();
        // Shared service runs with write-dedup ENABLED so testWriteDedup exercises the
        // skip path; the disabled path is covered by testWriteDedupDisabled.
        service = new OpcDaServiceImpl(fakeClient, true);
        server =
                InProcessServerBuilder.forName(name)
                        .directExecutor()
                        .addService(service)
                        .build()
                        .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    }

    @AfterAll
    static void teardown() {
        channel.shutdownNow();
        server.shutdownNow();
        service.shutdown();
    }

    @Test
    @DisplayName("GetStatus returns adapter status")
    void testGetStatus() {
        OpcDaServiceBlockingStub blocking = OpcDaServiceGrpc.newBlockingStub(channel);

        GetStatusResponse response = blocking.getStatus(GetStatusRequest.newBuilder().build());

        assertEquals(AdapterState.ADAPTER_STATE_HEALTHY, response.getState());
        assertEquals(1, response.getConnectionsCount());
        assertEquals("connection-1", response.getConnections(0).getConnectionId());
        assertEquals(
                ConnectionState.CONNECTION_STATE_CONNECTED, response.getConnections(0).getState());
    }

    @Test
    @DisplayName("Poll: ack returns per-item results and the stream emits data")
    void testPoll() throws Exception {
        OpcDaServiceStub asyncStub = OpcDaServiceGrpc.newStub(channel);

        CountDownLatch ackReceived = new CountDownLatch(1);
        CountDownLatch dataReceived = new CountDownLatch(1);
        AtomicReference<PollAck> received = new AtomicReference<>();
        io.grpc.Context.CancellableContext callContext =
                io.grpc.Context.current().withCancellation();

        callContext.run(
                () ->
                        asyncStub.poll(
                                pollRequest("poll-1", "connection-1", "Point1", "Point2"),
                                new StreamObserver<>() {
                                    @Override
                                    public void onNext(PollResponse response) {
                                        if (response.hasAck()) {
                                            received.set(response.getAck());
                                            ackReceived.countDown();
                                        } else if (response.hasData()) {
                                            dataReceived.countDown();
                                        }
                                    }

                                    @Override
                                    public void onError(Throwable error) {
                                        if (io.grpc.Status.fromThrowable(error).getCode()
                                                != io.grpc.Status.Code.CANCELLED) {
                                            fail("Poll error: " + error.getMessage());
                                        }
                                    }

                                    @Override
                                    public void onCompleted() {}
                                }));

        assertTrue(ackReceived.await(5, TimeUnit.SECONDS), "Poll ack timed out");
        assertTrue(dataReceived.await(5, TimeUnit.SECONDS), "Poll data timed out");
        PollAck ack = received.get();
        assertEquals(2, ack.getValidationsCount());

        ItemValidationResult r0 = ack.getValidations(0);
        assertEquals("Point1", r0.getItemId());
        assertFalse(r0.hasError());

        ItemValidationResult r1 = ack.getValidations(1);
        assertEquals("Point2", r1.getItemId());
        assertFalse(r1.hasError());
        callContext.cancel(null);
    }

    @Test
    @DisplayName("SetConnections: valid registry is accepted and request ID is echoed")
    void testSetConnectionsAcceptsValidRegistry() {
        OpcDaServiceBlockingStub blocking = OpcDaServiceGrpc.newBlockingStub(channel);

        SetConnectionsResponse response =
                blocking.setConnections(
                        SetConnectionsRequest.newBuilder()
                                .setRequestId("connections-1")
                                .addConnections(connection("registered-connection", 1_000))
                                .build());

        assertEquals("connections-1", response.getRequestId());
    }

    @Test
    @DisplayName("SetConnections: blank server identifier is rejected atomically")
    void testSetConnectionsRejectsBlankServerIdentifier() {
        OpcDaServiceBlockingStub blocking = OpcDaServiceGrpc.newBlockingStub(channel);

        var error =
                assertThrows(
                        io.grpc.StatusRuntimeException.class,
                        () ->
                                blocking.setConnections(
                                        SetConnectionsRequest.newBuilder()
                                                .setRequestId("blank-prog-id")
                                                .addConnections(
                                                        connection("blank-server-id", 1_000)
                                                                .toBuilder()
                                                                .setProgId(" "))
                                                .build()));

        assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
        assertEquals("prog_id must not be blank", error.getStatus().getDescription());
    }

    @Test
    @DisplayName("SetConnections: intervals above Java's supported range are rejected")
    void testSetConnectionsRejectsIntervalOverflow() {
        OpcDaServiceBlockingStub blocking = OpcDaServiceGrpc.newBlockingStub(channel);

        var error =
                assertThrows(
                        io.grpc.StatusRuntimeException.class,
                        () ->
                                blocking.setConnections(
                                        SetConnectionsRequest.newBuilder()
                                                .setRequestId("overflow")
                                                .addConnections(
                                                        connection("overflow-connection", 1_000)
                                                                .toBuilder()
                                                                .setPollIntervalMs(Long.MAX_VALUE))
                                                .build()));

        assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    @Test
    @DisplayName("Write: basic unary write returns per-point result")
    void testWrite() {
        OpcDaServiceBlockingStub blocking = OpcDaServiceGrpc.newBlockingStub(channel);

        WriteResponse response =
                blocking.write(
                        WriteRequest.newBuilder()
                                .setRequestId("wr-001")
                                .setTimeoutMs(5000)
                                .addTargets(
                                        WriteTarget.newBuilder()
                                                .setConnectionId("connection-1")
                                                .setItemId("Point1")
                                                .setDoubleValue(42.0)
                                                .build())
                                .build());

        assertEquals(1, response.getResultsCount());
        assertEquals("Point1", response.getResults(0).getItemId());
        assertEquals(
                WriteDisposition.WRITE_DISPOSITION_APPLIED,
                response.getResults(0).getDisposition());
    }

    @Test
    @DisplayName("Write: int16 values outside the wire type range are rejected")
    void testWriteRejectsOutOfRangeInt16() {
        OpcDaServiceBlockingStub blocking = OpcDaServiceGrpc.newBlockingStub(channel);
        int before = writeCount.get();

        WriteResponse response =
                blocking.write(
                        WriteRequest.newBuilder()
                                .setRequestId("wr-int16-range")
                                .setTimeoutMs(5000)
                                .addTargets(
                                        WriteTarget.newBuilder()
                                                .setConnectionId("connection-1")
                                                .setItemId("Point1")
                                                .setInt16Value(Short.MAX_VALUE + 1)
                                                .build())
                                .build());

        assertEquals(
                WriteDisposition.WRITE_DISPOSITION_FAILED, response.getResults(0).getDisposition());
        assertEquals(
                ErrorCode.ERROR_CODE_INVALID_ARGUMENT, response.getResults(0).getError().getCode());
        assertEquals(before, writeCount.get(), "invalid value must not reach the OPC client");
    }

    @Test
    @DisplayName("Write: uint64 is rejected instead of being narrowed to signed int64")
    void testWriteRejectsUnsupportedUint64() {
        OpcDaServiceBlockingStub blocking = OpcDaServiceGrpc.newBlockingStub(channel);
        int before = writeCount.get();

        WriteResponse response =
                blocking.write(
                        WriteRequest.newBuilder()
                                .setRequestId("wr-uint64")
                                .setTimeoutMs(5000)
                                .addTargets(
                                        WriteTarget.newBuilder()
                                                .setConnectionId("connection-1")
                                                .setItemId("Point1")
                                                .setUint64Value(1)
                                                .build())
                                .build());

        assertEquals(
                WriteDisposition.WRITE_DISPOSITION_FAILED, response.getResults(0).getDisposition());
        assertEquals(
                ErrorCode.ERROR_CODE_UNSUPPORTED_VALUE_TYPE,
                response.getResults(0).getError().getCode());
        assertEquals(before, writeCount.get(), "unsupported value must not reach the OPC client");
    }

    @Test
    @DisplayName("Write: duplicate value is skipped, different value is written")
    void testWriteDedup() {
        OpcDaServiceBlockingStub blocking = OpcDaServiceGrpc.newBlockingStub(channel);

        int before = writeCount.get();

        // Send three writes: new value, same value (skip), different value
        WriteResponse r1 = blocking.write(writeReq("DedupPoint", 7.0, "dedup-1"));
        WriteResponse r2 = blocking.write(writeReq("DedupPoint", 7.0, "dedup-2"));
        WriteResponse r3 = blocking.write(writeReq("DedupPoint", 9.0, "dedup-3"));

        // Only the first and third should reach the server; the duplicate is skipped
        assertEquals(2, writeCount.get() - before, "duplicate write should be skipped");
        assertEquals(WriteDisposition.WRITE_DISPOSITION_APPLIED, r1.getResults(0).getDisposition());
        assertEquals(WriteDisposition.WRITE_DISPOSITION_SKIPPED, r2.getResults(0).getDisposition());
        assertEquals(WriteDisposition.WRITE_DISPOSITION_APPLIED, r3.getResults(0).getDisposition());
    }

    private static WriteRequest writeReq(String itemId, double value, String reqId) {
        return WriteRequest.newBuilder()
                .setRequestId(reqId)
                .setTimeoutMs(5000)
                .addTargets(
                        WriteTarget.newBuilder()
                                .setConnectionId("connection-1")
                                .setItemId(itemId)
                                .setDoubleValue(value)
                                .build())
                .build();
    }

    private static PollRequest pollRequest(
            String requestId, String connectionId, String... itemIds) {
        PollRequest.Builder request =
                PollRequest.newBuilder().setRequestId(requestId).setConnectionId(connectionId);
        for (String itemId : itemIds) {
            request.addItemIds(itemId);
        }
        return request.build();
    }

    private static Connection connection(String connectionId, long pollIntervalMs) {
        return Connection.newBuilder()
                .setId(connectionId)
                .setHost("10.0.0.1")
                .setProgId("Test.OPC.1")
                .setPollIntervalMs(pollIntervalMs)
                .build();
    }

    @Test
    @DisplayName("Write: dedup disabled — duplicate values are all written")
    void testWriteDedupDisabled() throws Exception {
        // Dedicated service with dedup OFF (the production default).
        String localName = InProcessServerBuilder.generateName();
        var dedupOffService = new OpcDaServiceImpl(fakeClient, false);
        var localServer =
                InProcessServerBuilder.forName(localName)
                        .directExecutor()
                        .addService(dedupOffService)
                        .build()
                        .start();
        var localChannel = InProcessChannelBuilder.forName(localName).directExecutor().build();
        try {
            OpcDaServiceBlockingStub blocking = OpcDaServiceGrpc.newBlockingStub(localChannel);

            int before = writeCount.get();

            // Write the SAME value twice — both must reach the server when dedup is off.
            blocking.write(writeReq("NoDedupPoint", 5.0, "nodedup-1"));
            blocking.write(writeReq("NoDedupPoint", 5.0, "nodedup-2"));

            assertEquals(
                    2,
                    writeCount.get() - before,
                    "dedup disabled: both duplicate writes should reach the server");
        } finally {
            localChannel.shutdownNow();
            localServer.shutdownNow();
            dedupOffService.shutdown();
        }
    }

    @Test
    @DisplayName("Write: an item not currently polled still attempts a write")
    void testWriteUnknownItem() {
        OpcDaServiceBlockingStub blocking = OpcDaServiceGrpc.newBlockingStub(channel);

        WriteResponse response =
                blocking.write(
                        WriteRequest.newBuilder()
                                .setRequestId("wr-bad")
                                .setTimeoutMs(5000)
                                .addTargets(
                                        WriteTarget.newBuilder()
                                                .setConnectionId("connection-1")
                                                .setItemId("NonExistentItem")
                                                .setDoubleValue(1.0)
                                                .build())
                                .build());

        // fakeClient.write() returns success for any host/item.
        assertEquals(1, response.getResultsCount());
        assertEquals("NonExistentItem", response.getResults(0).getItemId());
    }

    @Test
    @DisplayName("Write: known HRESULT is returned as a stable domain error")
    void testWriteMapsKnownHresult() {
        var localService = new OpcDaServiceImpl(new MissingItemWriteClient(), false);
        try {
            WriteResponse response =
                    invokeWrite(localService, writeReq("MissingPoint", 1, "missing-item"));
            ErrorInfo error = response.getResults(0).getError();

            assertEquals(ErrorCode.ERROR_CODE_ITEM_NOT_FOUND, error.getCode());
            assertEquals("OPC item not found", error.getMessage());
            assertTrue(error.hasHresult());
            assertEquals(OpcDaHresult.UNKNOWN_ITEM_ID.value(), error.getHresult());
        } finally {
            localService.shutdown();
        }
    }

    @Test
    @DisplayName("Poll: blank itemId returns a failed validation result")
    void testPollBlankItemId() throws Exception {
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<PollAck> received = new AtomicReference<>();

        service.poll(
                pollRequest("blank-item", "blank-item-connection", ""),
                new StreamObserver<>() {
                    @Override
                    public void onNext(PollResponse response) {
                        if (response.hasAck()) {
                            received.set(response.getAck());
                        }
                    }

                    @Override
                    public void onError(Throwable error) {
                        fail("Poll error: " + error.getMessage());
                    }

                    @Override
                    public void onCompleted() {
                        completed.countDown();
                    }
                });

        assertTrue(completed.await(5, TimeUnit.SECONDS));
        ItemValidationResult result = received.get().getValidations(0);
        assertTrue(result.hasError());
        assertEquals("item_id is missing or duplicated", result.getError().getMessage());
    }

    @Test
    @DisplayName("Read: CACHE and DEVICE are forwarded to the OPC client")
    void testReadSource() {
        SourceTrackingClient client = new SourceTrackingClient();
        var localService = new OpcDaServiceImpl(client, false);
        try {
            ReadResponse cached =
                    invokeRead(localService, readRequest(ReadSource.READ_SOURCE_CACHE));
            assertFalse(client.lastDeviceRead.get());
            assertEquals(1, cached.getBatch().getValuesCount());

            ReadResponse device =
                    invokeRead(localService, readRequest(ReadSource.READ_SOURCE_DEVICE));
            assertTrue(client.lastDeviceRead.get());
            assertEquals(1, device.getBatch().getValuesCount());
        } finally {
            localService.shutdown();
        }
    }

    @Test
    @DisplayName("Read: a whole-operation failure is represented on the batch")
    void testReadConnectionFailureSetsBatchError() {
        var localService = new OpcDaServiceImpl(new ConnectionFailureReadClient(), false);
        try {
            ReadResponse response =
                    invokeRead(localService, readRequest(ReadSource.READ_SOURCE_DEVICE));

            assertTrue(response.getBatch().hasError());
            assertEquals(
                    ErrorCode.ERROR_CODE_OPC_FAILURE, response.getBatch().getError().getCode());
            assertEquals(0, response.getBatch().getValuesCount());
        } finally {
            localService.shutdown();
        }
    }

    @Test
    @DisplayName("Read: a per-item failure does not fail the whole batch")
    void testReadItemFailureStaysOnItem() {
        var localService = new OpcDaServiceImpl(new ItemFailureReadClient(), false);
        try {
            ReadResponse response =
                    invokeRead(localService, readRequest(ReadSource.READ_SOURCE_DEVICE));

            assertFalse(response.getBatch().hasError());
            assertEquals(1, response.getBatch().getValuesCount());
            assertTrue(response.getBatch().getValues(0).hasError());
            assertEquals(
                    ErrorCode.ERROR_CODE_ITEM_NOT_FOUND,
                    response.getBatch().getValues(0).getError().getCode());
        } finally {
            localService.shutdown();
        }
    }

    @Test
    @DisplayName("Poll: cancelling the stream stops future rounds")
    void testPollCancellationStopsScheduling() throws Exception {
        OpcDaServiceStub asyncStub = OpcDaServiceGrpc.newStub(channel);
        CountDownLatch firstData = new CountDownLatch(1);
        io.grpc.Context.CancellableContext callContext =
                io.grpc.Context.current().withCancellation();
        callContext.run(
                () ->
                        asyncStub.poll(
                                pollRequest("cancel-poll", "cancel-connection", "CancelPoint"),
                                new StreamObserver<>() {
                                    @Override
                                    public void onNext(PollResponse response) {
                                        if (response.hasData()) {
                                            firstData.countDown();
                                        }
                                    }

                                    @Override
                                    public void onError(Throwable error) {}

                                    @Override
                                    public void onCompleted() {}
                                }));
        assertTrue(firstData.await(5, TimeUnit.SECONDS));
        callContext.cancel(null);

        Thread.sleep(100);
        int stoppedAt = readCount.get();
        Thread.sleep(150);
        assertEquals(stoppedAt, readCount.get());
    }

    @Test
    @DisplayName("Write: late callers cannot bypass a connection lock with queued waiters")
    void testWriteConnectionLockRemainsStable() throws Exception {
        SerialWriteClient client = new SerialWriteClient();
        var localService = new OpcDaServiceImpl(client, false);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<WriteResponse> first =
                    executor.submit(
                            () -> invokeWrite(localService, writeReq("Lock-0", 1, "lock-0")));
            assertTrue(client.firstEntered.await(5, TimeUnit.SECONDS));

            List<Future<WriteResponse>> queued = new java.util.ArrayList<>();
            for (int index = 1; index <= 5; index++) {
                int item = index;
                queued.add(
                        executor.submit(
                                () ->
                                        invokeWrite(
                                                localService,
                                                writeReq("Lock-" + item, item, "lock-" + item))));
            }

            client.releaseFirst.countDown();
            assertTrue(client.secondEntered.await(5, TimeUnit.SECONDS));

            List<Future<WriteResponse>> late = new java.util.ArrayList<>();
            for (int index = 6; index <= 10; index++) {
                int item = index;
                late.add(
                        executor.submit(
                                () ->
                                        invokeWrite(
                                                localService,
                                                writeReq("Lock-" + item, item, "lock-" + item))));
            }

            assertEquals(
                    WriteDisposition.WRITE_DISPOSITION_APPLIED,
                    first.get(5, TimeUnit.SECONDS).getResults(0).getDisposition());
            for (Future<WriteResponse> response : queued) {
                assertEquals(
                        WriteDisposition.WRITE_DISPOSITION_APPLIED,
                        response.get(5, TimeUnit.SECONDS).getResults(0).getDisposition());
            }
            for (Future<WriteResponse> response : late) {
                assertEquals(
                        WriteDisposition.WRITE_DISPOSITION_APPLIED,
                        response.get(5, TimeUnit.SECONDS).getResults(0).getDisposition());
            }
            assertEquals(1, client.maxConcurrent.get());
        } finally {
            localService.shutdown();
        }
    }

    @Test
    @DisplayName("Write: deadline is rechecked after waiting for the connection lock")
    void testWriteDeadlineAfterConnectionLockWait() throws Exception {
        DeadlineWriteClient client = new DeadlineWriteClient();
        var localService = new OpcDaServiceImpl(client, false);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<WriteResponse> first =
                    executor.submit(
                            () -> invokeWrite(localService, writeReq("Slow", 1, "deadline-1")));
            assertTrue(client.firstEntered.await(5, TimeUnit.SECONDS));

            WriteRequest expiring =
                    WriteRequest.newBuilder()
                            .setRequestId("deadline-2")
                            .setTimeoutMs(100)
                            .addTargets(
                                    WriteTarget.newBuilder()
                                            .setConnectionId("connection-1")
                                            .setItemId("Expired")
                                            .setDoubleValue(2)
                                            .build())
                            .build();
            Future<WriteResponse> second =
                    executor.submit(() -> invokeWrite(localService, expiring));

            Thread.sleep(200);
            client.releaseFirst.countDown();

            assertEquals(
                    WriteDisposition.WRITE_DISPOSITION_APPLIED,
                    first.get(5, TimeUnit.SECONDS).getResults(0).getDisposition());
            WriteResponse expiredResponse = second.get(5, TimeUnit.SECONDS);
            assertEquals(
                    WriteDisposition.WRITE_DISPOSITION_FAILED,
                    expiredResponse.getResults(0).getDisposition());
            assertTrue(
                    expiredResponse
                            .getResults(0)
                            .getError()
                            .getMessage()
                            .contains("deadline exceeded"));
            assertEquals(1, client.calls.get(), "expired write must not reach the client");
        } finally {
            localService.shutdown();
        }
    }

    @Test
    @DisplayName("Write: reconnect generation invalidates dedup state")
    void testReconnectInvalidatesDedupState() {
        GenerationWriteClient client = new GenerationWriteClient();
        var localService = new OpcDaServiceImpl(client, true);
        try {
            assertEquals(
                    WriteDisposition.WRITE_DISPOSITION_APPLIED,
                    invokeWrite(localService, writeReq("GenerationPoint", 7, "generation-1"))
                            .getResults(0)
                            .getDisposition());
            assertEquals(
                    WriteDisposition.WRITE_DISPOSITION_SKIPPED,
                    invokeWrite(localService, writeReq("GenerationPoint", 7, "generation-2"))
                            .getResults(0)
                            .getDisposition());
            assertEquals(1, client.calls.get());

            client.generation.incrementAndGet();

            WriteResponse afterReconnect =
                    invokeWrite(localService, writeReq("GenerationPoint", 7, "generation-3"));
            assertEquals(
                    WriteDisposition.WRITE_DISPOSITION_APPLIED,
                    afterReconnect.getResults(0).getDisposition());
            assertEquals(2, client.calls.get());
        } finally {
            localService.shutdown();
        }
    }

    private static WriteResponse invokeWrite(OpcDaServiceImpl target, WriteRequest request) {
        AtomicReference<WriteResponse> response = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        target.write(
                request,
                new StreamObserver<>() {
                    @Override
                    public void onNext(WriteResponse value) {
                        response.set(value);
                    }

                    @Override
                    public void onError(Throwable failure) {
                        error.set(failure);
                    }

                    @Override
                    public void onCompleted() {}
                });
        assertNull(error.get());
        return assertInstanceOf(WriteResponse.class, response.get());
    }

    private static ReadRequest readRequest(ReadSource source) {
        return ReadRequest.newBuilder()
                .setRequestId("read-source")
                .setConnectionId("read-connection")
                .addItemIds("Point1")
                .setSource(source)
                .setTimeoutMs(5_000)
                .build();
    }

    private static ReadResponse invokeRead(OpcDaServiceImpl target, ReadRequest request) {
        AtomicReference<ReadResponse> response = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        target.read(
                request,
                new StreamObserver<>() {
                    @Override
                    public void onNext(ReadResponse value) {
                        response.set(value);
                    }

                    @Override
                    public void onError(Throwable failure) {
                        error.set(failure);
                    }

                    @Override
                    public void onCompleted() {}
                });
        assertNull(error.get());
        return assertInstanceOf(ReadResponse.class, response.get());
    }

    private static Map<String, Integer> successfulResults(Map<String, Object> values) {
        Map<String, Integer> results = new LinkedHashMap<>();
        values.keySet().forEach(itemId -> results.put(itemId, 0));
        return results;
    }

    private static final class SerialWriteClient extends OpcDaClient {
        final CountDownLatch firstEntered = new CountDownLatch(1);
        final CountDownLatch releaseFirst = new CountDownLatch(1);
        final CountDownLatch secondEntered = new CountDownLatch(1);
        final AtomicInteger calls = new AtomicInteger();
        final AtomicInteger active = new AtomicInteger();
        final AtomicInteger maxConcurrent = new AtomicInteger();

        SerialWriteClient() {
            super(1);
        }

        @Override
        public Map<String, Integer> write(
                String host, Map<String, Object> values, long deadlineMs) {
            int call = calls.incrementAndGet();
            int concurrent = active.incrementAndGet();
            maxConcurrent.accumulateAndGet(concurrent, Math::max);
            try {
                if (call == 1) {
                    firstEntered.countDown();
                    assertTrue(releaseFirst.await(5, TimeUnit.SECONDS));
                } else {
                    if (call == 2) {
                        secondEntered.countDown();
                    }
                    Thread.sleep(50);
                }
                return successfulResults(values);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                active.decrementAndGet();
            }
        }
    }

    private static final class DeadlineWriteClient extends OpcDaClient {
        final CountDownLatch firstEntered = new CountDownLatch(1);
        final CountDownLatch releaseFirst = new CountDownLatch(1);
        final AtomicInteger calls = new AtomicInteger();

        DeadlineWriteClient() {
            super(1);
        }

        @Override
        public Map<String, Integer> write(
                String host, Map<String, Object> values, long deadlineMs) {
            calls.incrementAndGet();
            firstEntered.countDown();
            try {
                assertTrue(releaseFirst.await(5, TimeUnit.SECONDS));
                return successfulResults(values);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    private static final class GenerationWriteClient extends OpcDaClient {
        final AtomicInteger calls = new AtomicInteger();
        final AtomicLong generation = new AtomicLong();

        GenerationWriteClient() {
            super(1);
        }

        @Override
        public long sessionGeneration(String host) {
            return generation.get();
        }

        @Override
        public Map<String, Integer> write(
                String host, Map<String, Object> values, long deadlineMs) {
            calls.incrementAndGet();
            return successfulResults(values);
        }
    }

    private static final class MissingItemWriteClient extends OpcDaClient {
        MissingItemWriteClient() {
            super(1);
        }

        @Override
        public Map<String, Integer> write(
                String connectionId, Map<String, Object> values, long deadlineMs) {
            Map<String, Integer> results = new LinkedHashMap<>();
            values.keySet()
                    .forEach(itemId -> results.put(itemId, OpcDaHresult.UNKNOWN_ITEM_ID.value()));
            return results;
        }
    }

    private static final class SourceTrackingClient extends OpcDaClient {
        final AtomicReference<Boolean> lastDeviceRead = new AtomicReference<>();

        SourceTrackingClient() {
            super(1);
        }

        @Override
        public boolean hasConnection(String connectionId) {
            return true;
        }

        @Override
        public String host(String connectionId) {
            return "10.0.0.1";
        }

        @Override
        public long getServerTimeMs(String connectionId) {
            return 0;
        }

        @Override
        public long getServerTimeMs(String connectionId, long deadlineMs) {
            return 0;
        }

        @Override
        public List<ReadResult> read(
                String connectionId, List<String> itemIds, boolean device, long deadlineMs) {
            lastDeviceRead.set(device);
            return itemIds.stream().map(itemId -> ReadResult.success(itemId, 1, 0xC0, 1)).toList();
        }
    }

    private abstract static class ReadBoundaryClient extends OpcDaClient {
        ReadBoundaryClient() {
            super(1);
        }

        @Override
        public boolean hasConnection(String connectionId) {
            return true;
        }

        @Override
        public String host(String connectionId) {
            return "10.0.0.1";
        }

        @Override
        public long getServerTimeMs(String connectionId, long deadlineMs) {
            return 0;
        }
    }

    private static final class ConnectionFailureReadClient extends ReadBoundaryClient {
        @Override
        public List<ReadResult> read(
                String connectionId, List<String> itemIds, boolean device, long deadlineMs) {
            throw new OpcDaClient.ReadOperationException(
                    ErrorCode.ERROR_CODE_OPC_FAILURE, "connection failed");
        }
    }

    private static final class ItemFailureReadClient extends ReadBoundaryClient {
        @Override
        public List<ReadResult> read(
                String connectionId, List<String> itemIds, boolean device, long deadlineMs) {
            return itemIds.stream()
                    .map(
                            itemId ->
                                    ReadResult.failure(
                                            itemId,
                                            0,
                                            0,
                                            ErrorCode.ERROR_CODE_ITEM_NOT_FOUND,
                                            "missing"))
                    .toList();
        }
    }
}
