/* (C) 2026 */
package com.dalugm.opcdapter;

import io.micronaut.context.ApplicationContext;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;

import jakarta.inject.Inject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@MicronautTest
class OpcdapterTest {

    @Inject ApplicationContext context;

    @Test
    void testItWorks() {
        Assertions.assertTrue(context.isRunning());
    }
}
