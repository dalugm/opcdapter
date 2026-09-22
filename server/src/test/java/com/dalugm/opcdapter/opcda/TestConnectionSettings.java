/* (C) 2026 */
package com.dalugm.opcdapter.opcda;

import java.util.Map;

import com.dalugm.opcdapter.api.opcda.v1.Connection;
import com.dalugm.opcdapter.api.opcda.v1.ConnectionCredentials;

final class TestConnectionSettings {

    private TestConnectionSettings() {}

    static Connection load() {
        return load(System.getenv());
    }

    static Connection load(Map<String, String> environment) {
        String clsId = environment.getOrDefault("OPCDA_CLS_ID", "").trim();
        String progId = environment.getOrDefault("OPCDA_PROG_ID", "").trim();
        if (clsId.isBlank() == progId.isBlank()) {
            throw new IllegalArgumentException("set exactly one of OPCDA_CLS_ID or OPCDA_PROG_ID");
        }

        Connection.Builder connection =
                Connection.newBuilder()
                        .setId(environment.getOrDefault("OPCDA_CONNECTION_ID", "live-test"))
                        .setHost(require(environment, "OPCDA_HOST"))
                        .setDomain(environment.getOrDefault("OPCDA_DOMAIN", ""))
                        .setUsername(environment.getOrDefault("OPCDA_USERNAME", ""))
                        .setCredentials(
                                ConnectionCredentials.newBuilder()
                                        .setPassword(
                                                environment.getOrDefault("OPCDA_PASSWORD", "")))
                        .setPollIntervalMs(
                                Long.parseLong(
                                        environment.getOrDefault(
                                                "OPCDA_POLL_INTERVAL_MS", "1000")));
        if (!clsId.isBlank()) {
            connection.setClsId(clsId);
        } else {
            connection.setProgId(progId);
        }
        return connection.build();
    }

    private static String require(Map<String, String> environment, String name) {
        String value = environment.getOrDefault(name, "").trim();
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must be set");
        }
        return value;
    }
}
