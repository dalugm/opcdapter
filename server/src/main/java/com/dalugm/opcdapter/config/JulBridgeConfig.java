/* (C) 2026 */
package com.dalugm.opcdapter.config;

import io.micronaut.context.annotation.Context;

import jakarta.annotation.PostConstruct;

import org.slf4j.bridge.SLF4JBridgeHandler;

@Context
public class JulBridgeConfig {

    @PostConstruct
    void install() {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }
}
