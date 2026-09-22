/* (C) 2026 */
package com.dalugm.opcdapter.grpc;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class GrpcAuthInterceptorTest {

    @Test
    void comparesConfiguredToken() {
        GrpcAuthInterceptor interceptor = new GrpcAuthInterceptor("correct-horse-battery-staple");

        assertTrue(interceptor.matches("correct-horse-battery-staple"));
        assertFalse(interceptor.matches("wrong"));
        assertFalse(interceptor.matches(null));
    }

    @Test
    void rejectsBlankConfiguredToken() {
        assertThrows(IllegalArgumentException.class, () -> new GrpcAuthInterceptor("  "));
    }
}
