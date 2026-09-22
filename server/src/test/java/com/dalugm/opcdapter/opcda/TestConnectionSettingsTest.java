/* (C) 2026 */
package com.dalugm.opcdapter.opcda;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.junit.jupiter.api.Test;

class TestConnectionSettingsTest {

    @Test
    void buildsConnectionFromExplicitEnvironment() {
        var connection =
                TestConnectionSettings.load(
                        Map.of(
                                "OPCDA_HOST", "10.0.0.1",
                                "OPCDA_PROG_ID", "Factory.OPC.1",
                                "OPCDA_USERNAME", "operator",
                                "OPCDA_PASSWORD", "secret",
                                "OPCDA_POLL_INTERVAL_MS", "750"));

        assertEquals("live-test", connection.getId());
        assertEquals("10.0.0.1", connection.getHost());
        assertEquals("Factory.OPC.1", connection.getProgId());
        assertEquals("secret", connection.getCredentials().getPassword());
        assertEquals(750, connection.getPollIntervalMs());
    }
}
