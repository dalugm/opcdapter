/* (C) 2026 */
package com.dalugm.opcdapter.opcda;

import static org.junit.jupiter.api.Assertions.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.micronaut.context.ApplicationContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@EnabledIfSystemProperty(named = "opcda.it.enabled", matches = "true")
class OpcDaWriteTest {

    private static final Logger log = LoggerFactory.getLogger(OpcDaWriteTest.class);

    @Test
    void writePoint() throws Exception {
        var connection = TestConnectionSettings.load();
        String pairsText = System.getenv().getOrDefault("OPCDA_PAIRS", "");

        assertFalse(
                pairsText.isBlank(),
                "OPCDA_PAIRS must be set (e.g. ITEM.PV=5.0 or \"A.PV=1.0,B.PV=2.0\")");

        Map<String, Double> writes = parsePairs(pairsText);
        log.info("WRITE target '{}' / {} point(s)", connection.getId(), writes.size());

        try (ApplicationContext ctx = ApplicationContext.run()) {
            OpcDaClient client = ctx.getBean(OpcDaClient.class);
            client.setConnections(List.of(connection));

            // Batch write all points in one DCOM call
            Map<String, Object> values = new LinkedHashMap<>(writes);
            var writeResults = client.write(connection.getId(), values);
            for (var entry : writeResults.entrySet()) {
                log.info(
                        "write {} = {} (HRESULT 0x{})",
                        entry.getKey(),
                        writes.get(entry.getKey()),
                        Integer.toHexString(entry.getValue()));
            }

            // Batch read-back all points in one DCOM call
            var readResults = client.read(connection.getId(), writes.keySet().stream().toList());
            for (var r : readResults) {
                log.info(
                        "read-back {} = {} (quality=0x{}, good={})",
                        r.itemId(),
                        r.value(),
                        Integer.toHexString(r.quality()),
                        r.isGood());
                assertNull(r.error(), "read-back error for " + r.itemId() + ": " + r.error());
            }
        }
    }

    private static Map<String, Double> parsePairs(String text) {
        Map<String, Double> result = new LinkedHashMap<>();
        for (String pair : text.split(",")) {
            pair = pair.trim();
            if (pair.isEmpty()) continue;
            int eq = pair.lastIndexOf('=');
            if (eq < 0) {
                throw new IllegalArgumentException(
                        "invalid pair '" + pair + "', expected item=value");
            }
            String itemId = pair.substring(0, eq).trim();
            double value = Double.parseDouble(pair.substring(eq + 1).trim());
            result.put(itemId, value);
        }
        if (result.isEmpty()) {
            throw new IllegalArgumentException("no item=value pairs found in: " + text);
        }
        return result;
    }
}
