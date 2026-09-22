/* (C) 2026 */
package com.dalugm.opcdapter.opcda;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import io.micronaut.context.ApplicationContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@EnabledIfSystemProperty(named = "opcda.it.enabled", matches = "true")
class OpcDaServerTimeTest {

    private static final Logger log = LoggerFactory.getLogger(OpcDaServerTimeTest.class);

    @Test
    void readServerTime() throws Exception {
        var connection = TestConnectionSettings.load();

        try (ApplicationContext ctx = ApplicationContext.run()) {
            OpcDaClient client = ctx.getBean(OpcDaClient.class);
            client.setConnections(List.of(connection));

            long serverTimeMs = client.getServerTimeMs(connection.getId());
            long nowMs = System.currentTimeMillis();

            log.info(
                    "OPC DA server '{}' current time: {} (diff from local: {}ms)",
                    connection.getId(),
                    new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
                            .format(new java.util.Date(serverTimeMs)),
                    serverTimeMs - nowMs);

            assertTrue(serverTimeMs > 0, "server time must be > 0");
            assertTrue(
                    Math.abs(serverTimeMs - nowMs) < 3_600_000,
                    "server time should be within 1 hour of local time");
        }
    }
}
