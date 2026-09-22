/* (C) 2026 */
package com.dalugm.opcdapter.opcda;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;

import io.micronaut.context.ApplicationContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@EnabledIfSystemProperty(named = "opcda.it.enabled", matches = "true")
class OpcDaReadTest {

    private static final Logger log = LoggerFactory.getLogger(OpcDaReadTest.class);

    @Test
    void readPoints() throws Exception {
        var connection = TestConnectionSettings.load();
        String connectionId = connection.getId();
        String host = connection.getHost();
        List<String> items = resolveItems();

        try (ApplicationContext ctx = ApplicationContext.run()) {
            OpcDaClient client = ctx.getBean(OpcDaClient.class);
            client.setConnections(List.of(connection));
            log.info(
                    "polling {} item(s) from '{}' (host={}) every 1s for 10s",
                    items.size(),
                    connectionId,
                    host);

            long deadline = System.currentTimeMillis() + 10_000;
            int round = 0;
            while (System.currentTimeMillis() < deadline) {
                round++;
                long roundStart = System.currentTimeMillis();
                List<ReadResult> results = client.read(connectionId, items);
                long elapsed = System.currentTimeMillis() - roundStart;

                long goodCount = 0;
                for (ReadResult r : results) {
                    if (r.error() != null) {
                        System.out.printf("[%d] %s ERROR: %s%n", round, r.itemId(), r.error());
                    } else if (r.isGood()) {
                        goodCount++;
                        System.out.printf(
                                "[%d] %s = %s (quality=0x%02x)%n",
                                round, r.itemId(), r.value(), r.quality());
                    } else {
                        System.out.printf(
                                "[%d] %s BAD quality=0x%02x%n", round, r.itemId(), r.quality());
                    }
                }
                System.out.printf(
                        "--- round %d: %d/%d GOOD (%dms) ---%n",
                        round, goodCount, results.size(), elapsed);

                long remaining = 1000 - (System.currentTimeMillis() - roundStart);
                if (remaining > 0) {
                    Thread.sleep(remaining);
                }
            }
            log.info("polling complete: {} rounds", round);
        }
    }

    private static List<String> resolveItems() {
        String raw = System.getenv().getOrDefault("OPCDA_ITEMS", "");
        assertFalse(raw.isBlank(), "OPCDA_ITEMS must contain comma-separated Item IDs");
        return Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(item -> !item.isEmpty())
                .toList();
    }
}
