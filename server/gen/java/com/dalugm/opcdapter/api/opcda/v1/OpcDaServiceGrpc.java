package com.dalugm.opcdapter.api.opcda.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class OpcDaServiceGrpc {

  private OpcDaServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "opcda.v1.OpcDaService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.dalugm.opcdapter.api.opcda.v1.SetConnectionsRequest,
      com.dalugm.opcdapter.api.opcda.v1.SetConnectionsResponse> getSetConnectionsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetConnections",
      requestType = com.dalugm.opcdapter.api.opcda.v1.SetConnectionsRequest.class,
      responseType = com.dalugm.opcdapter.api.opcda.v1.SetConnectionsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.dalugm.opcdapter.api.opcda.v1.SetConnectionsRequest,
      com.dalugm.opcdapter.api.opcda.v1.SetConnectionsResponse> getSetConnectionsMethod() {
    io.grpc.MethodDescriptor<com.dalugm.opcdapter.api.opcda.v1.SetConnectionsRequest, com.dalugm.opcdapter.api.opcda.v1.SetConnectionsResponse> getSetConnectionsMethod;
    if ((getSetConnectionsMethod = OpcDaServiceGrpc.getSetConnectionsMethod) == null) {
      synchronized (OpcDaServiceGrpc.class) {
        if ((getSetConnectionsMethod = OpcDaServiceGrpc.getSetConnectionsMethod) == null) {
          OpcDaServiceGrpc.getSetConnectionsMethod = getSetConnectionsMethod =
              io.grpc.MethodDescriptor.<com.dalugm.opcdapter.api.opcda.v1.SetConnectionsRequest, com.dalugm.opcdapter.api.opcda.v1.SetConnectionsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetConnections"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dalugm.opcdapter.api.opcda.v1.SetConnectionsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dalugm.opcdapter.api.opcda.v1.SetConnectionsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new OpcDaServiceMethodDescriptorSupplier("SetConnections"))
              .build();
        }
      }
    }
    return getSetConnectionsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.dalugm.opcdapter.api.opcda.v1.ReadRequest,
      com.dalugm.opcdapter.api.opcda.v1.ReadResponse> getReadMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Read",
      requestType = com.dalugm.opcdapter.api.opcda.v1.ReadRequest.class,
      responseType = com.dalugm.opcdapter.api.opcda.v1.ReadResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.dalugm.opcdapter.api.opcda.v1.ReadRequest,
      com.dalugm.opcdapter.api.opcda.v1.ReadResponse> getReadMethod() {
    io.grpc.MethodDescriptor<com.dalugm.opcdapter.api.opcda.v1.ReadRequest, com.dalugm.opcdapter.api.opcda.v1.ReadResponse> getReadMethod;
    if ((getReadMethod = OpcDaServiceGrpc.getReadMethod) == null) {
      synchronized (OpcDaServiceGrpc.class) {
        if ((getReadMethod = OpcDaServiceGrpc.getReadMethod) == null) {
          OpcDaServiceGrpc.getReadMethod = getReadMethod =
              io.grpc.MethodDescriptor.<com.dalugm.opcdapter.api.opcda.v1.ReadRequest, com.dalugm.opcdapter.api.opcda.v1.ReadResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Read"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dalugm.opcdapter.api.opcda.v1.ReadRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dalugm.opcdapter.api.opcda.v1.ReadResponse.getDefaultInstance()))
              .setSchemaDescriptor(new OpcDaServiceMethodDescriptorSupplier("Read"))
              .build();
        }
      }
    }
    return getReadMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.dalugm.opcdapter.api.opcda.v1.PollRequest,
      com.dalugm.opcdapter.api.opcda.v1.PollResponse> getPollMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Poll",
      requestType = com.dalugm.opcdapter.api.opcda.v1.PollRequest.class,
      responseType = com.dalugm.opcdapter.api.opcda.v1.PollResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<com.dalugm.opcdapter.api.opcda.v1.PollRequest,
      com.dalugm.opcdapter.api.opcda.v1.PollResponse> getPollMethod() {
    io.grpc.MethodDescriptor<com.dalugm.opcdapter.api.opcda.v1.PollRequest, com.dalugm.opcdapter.api.opcda.v1.PollResponse> getPollMethod;
    if ((getPollMethod = OpcDaServiceGrpc.getPollMethod) == null) {
      synchronized (OpcDaServiceGrpc.class) {
        if ((getPollMethod = OpcDaServiceGrpc.getPollMethod) == null) {
          OpcDaServiceGrpc.getPollMethod = getPollMethod =
              io.grpc.MethodDescriptor.<com.dalugm.opcdapter.api.opcda.v1.PollRequest, com.dalugm.opcdapter.api.opcda.v1.PollResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Poll"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dalugm.opcdapter.api.opcda.v1.PollRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dalugm.opcdapter.api.opcda.v1.PollResponse.getDefaultInstance()))
              .setSchemaDescriptor(new OpcDaServiceMethodDescriptorSupplier("Poll"))
              .build();
        }
      }
    }
    return getPollMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.dalugm.opcdapter.api.opcda.v1.WriteRequest,
      com.dalugm.opcdapter.api.opcda.v1.WriteResponse> getWriteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Write",
      requestType = com.dalugm.opcdapter.api.opcda.v1.WriteRequest.class,
      responseType = com.dalugm.opcdapter.api.opcda.v1.WriteResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.dalugm.opcdapter.api.opcda.v1.WriteRequest,
      com.dalugm.opcdapter.api.opcda.v1.WriteResponse> getWriteMethod() {
    io.grpc.MethodDescriptor<com.dalugm.opcdapter.api.opcda.v1.WriteRequest, com.dalugm.opcdapter.api.opcda.v1.WriteResponse> getWriteMethod;
    if ((getWriteMethod = OpcDaServiceGrpc.getWriteMethod) == null) {
      synchronized (OpcDaServiceGrpc.class) {
        if ((getWriteMethod = OpcDaServiceGrpc.getWriteMethod) == null) {
          OpcDaServiceGrpc.getWriteMethod = getWriteMethod =
              io.grpc.MethodDescriptor.<com.dalugm.opcdapter.api.opcda.v1.WriteRequest, com.dalugm.opcdapter.api.opcda.v1.WriteResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Write"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dalugm.opcdapter.api.opcda.v1.WriteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dalugm.opcdapter.api.opcda.v1.WriteResponse.getDefaultInstance()))
              .setSchemaDescriptor(new OpcDaServiceMethodDescriptorSupplier("Write"))
              .build();
        }
      }
    }
    return getWriteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.dalugm.opcdapter.api.opcda.v1.GetStatusRequest,
      com.dalugm.opcdapter.api.opcda.v1.GetStatusResponse> getGetStatusMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetStatus",
      requestType = com.dalugm.opcdapter.api.opcda.v1.GetStatusRequest.class,
      responseType = com.dalugm.opcdapter.api.opcda.v1.GetStatusResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.dalugm.opcdapter.api.opcda.v1.GetStatusRequest,
      com.dalugm.opcdapter.api.opcda.v1.GetStatusResponse> getGetStatusMethod() {
    io.grpc.MethodDescriptor<com.dalugm.opcdapter.api.opcda.v1.GetStatusRequest, com.dalugm.opcdapter.api.opcda.v1.GetStatusResponse> getGetStatusMethod;
    if ((getGetStatusMethod = OpcDaServiceGrpc.getGetStatusMethod) == null) {
      synchronized (OpcDaServiceGrpc.class) {
        if ((getGetStatusMethod = OpcDaServiceGrpc.getGetStatusMethod) == null) {
          OpcDaServiceGrpc.getGetStatusMethod = getGetStatusMethod =
              io.grpc.MethodDescriptor.<com.dalugm.opcdapter.api.opcda.v1.GetStatusRequest, com.dalugm.opcdapter.api.opcda.v1.GetStatusResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetStatus"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dalugm.opcdapter.api.opcda.v1.GetStatusRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.dalugm.opcdapter.api.opcda.v1.GetStatusResponse.getDefaultInstance()))
              .setSchemaDescriptor(new OpcDaServiceMethodDescriptorSupplier("GetStatus"))
              .build();
        }
      }
    }
    return getGetStatusMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static OpcDaServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<OpcDaServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<OpcDaServiceStub>() {
        @java.lang.Override
        public OpcDaServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new OpcDaServiceStub(channel, callOptions);
        }
      };
    return OpcDaServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static OpcDaServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<OpcDaServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<OpcDaServiceBlockingV2Stub>() {
        @java.lang.Override
        public OpcDaServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new OpcDaServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return OpcDaServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static OpcDaServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<OpcDaServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<OpcDaServiceBlockingStub>() {
        @java.lang.Override
        public OpcDaServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new OpcDaServiceBlockingStub(channel, callOptions);
        }
      };
    return OpcDaServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static OpcDaServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<OpcDaServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<OpcDaServiceFutureStub>() {
        @java.lang.Override
        public OpcDaServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new OpcDaServiceFutureStub(channel, callOptions);
        }
      };
    return OpcDaServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void setConnections(com.dalugm.opcdapter.api.opcda.v1.SetConnectionsRequest request,
        io.grpc.stub.StreamObserver<com.dalugm.opcdapter.api.opcda.v1.SetConnectionsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSetConnectionsMethod(), responseObserver);
    }

    /**
     */
    default void read(com.dalugm.opcdapter.api.opcda.v1.ReadRequest request,
        io.grpc.stub.StreamObserver<com.dalugm.opcdapter.api.opcda.v1.ReadResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReadMethod(), responseObserver);
    }

    /**
     */
    default void poll(com.dalugm.opcdapter.api.opcda.v1.PollRequest request,
        io.grpc.stub.StreamObserver<com.dalugm.opcdapter.api.opcda.v1.PollResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPollMethod(), responseObserver);
    }

    /**
     */
    default void write(com.dalugm.opcdapter.api.opcda.v1.WriteRequest request,
        io.grpc.stub.StreamObserver<com.dalugm.opcdapter.api.opcda.v1.WriteResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getWriteMethod(), responseObserver);
    }

    /**
     */
    default void getStatus(com.dalugm.opcdapter.api.opcda.v1.GetStatusRequest request,
        io.grpc.stub.StreamObserver<com.dalugm.opcdapter.api.opcda.v1.GetStatusResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetStatusMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service OpcDaService.
   */
  public static abstract class OpcDaServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return OpcDaServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service OpcDaService.
   */
  public static final class OpcDaServiceStub
      extends io.grpc.stub.AbstractAsyncStub<OpcDaServiceStub> {
    private OpcDaServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected OpcDaServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new OpcDaServiceStub(channel, callOptions);
    }

    /**
     */
    public void setConnections(com.dalugm.opcdapter.api.opcda.v1.SetConnectionsRequest request,
        io.grpc.stub.StreamObserver<com.dalugm.opcdapter.api.opcda.v1.SetConnectionsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSetConnectionsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void read(com.dalugm.opcdapter.api.opcda.v1.ReadRequest request,
        io.grpc.stub.StreamObserver<com.dalugm.opcdapter.api.opcda.v1.ReadResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReadMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void poll(com.dalugm.opcdapter.api.opcda.v1.PollRequest request,
        io.grpc.stub.StreamObserver<com.dalugm.opcdapter.api.opcda.v1.PollResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getPollMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void write(com.dalugm.opcdapter.api.opcda.v1.WriteRequest request,
        io.grpc.stub.StreamObserver<com.dalugm.opcdapter.api.opcda.v1.WriteResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getWriteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getStatus(com.dalugm.opcdapter.api.opcda.v1.GetStatusRequest request,
        io.grpc.stub.StreamObserver<com.dalugm.opcdapter.api.opcda.v1.GetStatusResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetStatusMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service OpcDaService.
   */
  public static final class OpcDaServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<OpcDaServiceBlockingV2Stub> {
    private OpcDaServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected OpcDaServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new OpcDaServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     */
    public com.dalugm.opcdapter.api.opcda.v1.SetConnectionsResponse setConnections(com.dalugm.opcdapter.api.opcda.v1.SetConnectionsRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getSetConnectionsMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.dalugm.opcdapter.api.opcda.v1.ReadResponse read(com.dalugm.opcdapter.api.opcda.v1.ReadRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getReadMethod(), getCallOptions(), request);
    }

    /**
     */
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/10918")
    public io.grpc.stub.BlockingClientCall<?, com.dalugm.opcdapter.api.opcda.v1.PollResponse>
        poll(com.dalugm.opcdapter.api.opcda.v1.PollRequest request) {
      return io.grpc.stub.ClientCalls.blockingV2ServerStreamingCall(
          getChannel(), getPollMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.dalugm.opcdapter.api.opcda.v1.WriteResponse write(com.dalugm.opcdapter.api.opcda.v1.WriteRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getWriteMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.dalugm.opcdapter.api.opcda.v1.GetStatusResponse getStatus(com.dalugm.opcdapter.api.opcda.v1.GetStatusRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getGetStatusMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service OpcDaService.
   */
  public static final class OpcDaServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<OpcDaServiceBlockingStub> {
    private OpcDaServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected OpcDaServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new OpcDaServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.dalugm.opcdapter.api.opcda.v1.SetConnectionsResponse setConnections(com.dalugm.opcdapter.api.opcda.v1.SetConnectionsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSetConnectionsMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.dalugm.opcdapter.api.opcda.v1.ReadResponse read(com.dalugm.opcdapter.api.opcda.v1.ReadRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReadMethod(), getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.dalugm.opcdapter.api.opcda.v1.PollResponse> poll(
        com.dalugm.opcdapter.api.opcda.v1.PollRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getPollMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.dalugm.opcdapter.api.opcda.v1.WriteResponse write(com.dalugm.opcdapter.api.opcda.v1.WriteRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getWriteMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.dalugm.opcdapter.api.opcda.v1.GetStatusResponse getStatus(com.dalugm.opcdapter.api.opcda.v1.GetStatusRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetStatusMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service OpcDaService.
   */
  public static final class OpcDaServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<OpcDaServiceFutureStub> {
    private OpcDaServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected OpcDaServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new OpcDaServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.dalugm.opcdapter.api.opcda.v1.SetConnectionsResponse> setConnections(
        com.dalugm.opcdapter.api.opcda.v1.SetConnectionsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSetConnectionsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.dalugm.opcdapter.api.opcda.v1.ReadResponse> read(
        com.dalugm.opcdapter.api.opcda.v1.ReadRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReadMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.dalugm.opcdapter.api.opcda.v1.WriteResponse> write(
        com.dalugm.opcdapter.api.opcda.v1.WriteRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getWriteMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.dalugm.opcdapter.api.opcda.v1.GetStatusResponse> getStatus(
        com.dalugm.opcdapter.api.opcda.v1.GetStatusRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetStatusMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SET_CONNECTIONS = 0;
  private static final int METHODID_READ = 1;
  private static final int METHODID_POLL = 2;
  private static final int METHODID_WRITE = 3;
  private static final int METHODID_GET_STATUS = 4;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SET_CONNECTIONS:
          serviceImpl.setConnections((com.dalugm.opcdapter.api.opcda.v1.SetConnectionsRequest) request,
              (io.grpc.stub.StreamObserver<com.dalugm.opcdapter.api.opcda.v1.SetConnectionsResponse>) responseObserver);
          break;
        case METHODID_READ:
          serviceImpl.read((com.dalugm.opcdapter.api.opcda.v1.ReadRequest) request,
              (io.grpc.stub.StreamObserver<com.dalugm.opcdapter.api.opcda.v1.ReadResponse>) responseObserver);
          break;
        case METHODID_POLL:
          serviceImpl.poll((com.dalugm.opcdapter.api.opcda.v1.PollRequest) request,
              (io.grpc.stub.StreamObserver<com.dalugm.opcdapter.api.opcda.v1.PollResponse>) responseObserver);
          break;
        case METHODID_WRITE:
          serviceImpl.write((com.dalugm.opcdapter.api.opcda.v1.WriteRequest) request,
              (io.grpc.stub.StreamObserver<com.dalugm.opcdapter.api.opcda.v1.WriteResponse>) responseObserver);
          break;
        case METHODID_GET_STATUS:
          serviceImpl.getStatus((com.dalugm.opcdapter.api.opcda.v1.GetStatusRequest) request,
              (io.grpc.stub.StreamObserver<com.dalugm.opcdapter.api.opcda.v1.GetStatusResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getSetConnectionsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.dalugm.opcdapter.api.opcda.v1.SetConnectionsRequest,
              com.dalugm.opcdapter.api.opcda.v1.SetConnectionsResponse>(
                service, METHODID_SET_CONNECTIONS)))
        .addMethod(
          getReadMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.dalugm.opcdapter.api.opcda.v1.ReadRequest,
              com.dalugm.opcdapter.api.opcda.v1.ReadResponse>(
                service, METHODID_READ)))
        .addMethod(
          getPollMethod(),
          io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new MethodHandlers<
              com.dalugm.opcdapter.api.opcda.v1.PollRequest,
              com.dalugm.opcdapter.api.opcda.v1.PollResponse>(
                service, METHODID_POLL)))
        .addMethod(
          getWriteMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.dalugm.opcdapter.api.opcda.v1.WriteRequest,
              com.dalugm.opcdapter.api.opcda.v1.WriteResponse>(
                service, METHODID_WRITE)))
        .addMethod(
          getGetStatusMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.dalugm.opcdapter.api.opcda.v1.GetStatusRequest,
              com.dalugm.opcdapter.api.opcda.v1.GetStatusResponse>(
                service, METHODID_GET_STATUS)))
        .build();
  }

  private static abstract class OpcDaServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    OpcDaServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.dalugm.opcdapter.api.opcda.v1.Opcda.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("OpcDaService");
    }
  }

  private static final class OpcDaServiceFileDescriptorSupplier
      extends OpcDaServiceBaseDescriptorSupplier {
    OpcDaServiceFileDescriptorSupplier() {}
  }

  private static final class OpcDaServiceMethodDescriptorSupplier
      extends OpcDaServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    OpcDaServiceMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (OpcDaServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new OpcDaServiceFileDescriptorSupplier())
              .addMethod(getSetConnectionsMethod())
              .addMethod(getReadMethod())
              .addMethod(getPollMethod())
              .addMethod(getWriteMethod())
              .addMethod(getGetStatusMethod())
              .build();
        }
      }
    }
    return result;
  }
}
