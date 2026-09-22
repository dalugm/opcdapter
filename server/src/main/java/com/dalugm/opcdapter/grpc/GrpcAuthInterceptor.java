/* (C) 2026 */
package com.dalugm.opcdapter.grpc;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;

import jakarta.inject.Singleton;

/** Optional shared-token authentication for deployments that expose gRPC beyond loopback. */
@Singleton
@Requires(property = "opcdapter.grpc.auth-token")
public final class GrpcAuthInterceptor implements ServerInterceptor {
    static final Metadata.Key<String> TOKEN_HEADER =
            Metadata.Key.of("x-opcdapter-token", Metadata.ASCII_STRING_MARSHALLER);

    private final byte[] expectedToken;

    public GrpcAuthInterceptor(@Value("${opcdapter.grpc.auth-token}") String token) {
        if (token.isBlank()) {
            throw new IllegalArgumentException("opcdapter.grpc.auth-token must not be blank");
        }
        expectedToken = token.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public <RequestT, ResponseT> ServerCall.Listener<RequestT> interceptCall(
            ServerCall<RequestT, ResponseT> call,
            Metadata headers,
            ServerCallHandler<RequestT, ResponseT> next) {
        String suppliedToken = headers.get(TOKEN_HEADER);
        if (!matches(suppliedToken)) {
            call.close(
                    Status.UNAUTHENTICATED.withDescription("invalid gRPC token"), new Metadata());
            return new ServerCall.Listener<>() {};
        }
        return next.startCall(call, headers);
    }

    boolean matches(String suppliedToken) {
        return suppliedToken != null
                && MessageDigest.isEqual(
                        expectedToken, suppliedToken.getBytes(StandardCharsets.UTF_8));
    }
}
