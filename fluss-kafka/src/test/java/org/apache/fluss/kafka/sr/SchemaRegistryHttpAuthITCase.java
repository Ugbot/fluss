/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka.sr;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end coverage for the Schema Registry HTTP principal-extraction trust paths (Phase G.4).
 * Runs against a live coordinator-hosted SR listener with RBAC enforced but no grants pre-seeded —
 * every SR call must therefore come back {@code 403} (the catalog denies ANONYMOUS), and the 403
 * body's {@code "message"} field reveals which principal the extractor handed the service. That's
 * the signal we assert against:
 *
 * <ul>
 *   <li>{@code X-Forwarded-User: alice} from loopback → body says {@code "principal 'alice'"}.
 *   <li>HTTP Basic {@code alice:alice-secret} → body says {@code "principal 'alice'"}.
 *   <li>Wrong Basic credentials → body says {@code "principal 'ANONYMOUS'"} (extractor returned
 *       empty, fall-back kicked in).
 *   <li>No credentials → body says {@code "principal 'ANONYMOUS'"}.
 *   <li>{@code X-Forwarded-User} present AND a wrong Basic header → header wins, body still says
 *       {@code "principal 'alice'"}.
 * </ul>
 *
 * <p>We deliberately avoid granting alice anything. Seeding a real grant requires the catalog's
 * internal {@code __principals__} / {@code __grants__} PK tables to be fully ready, which races
 * with the coordinator's lazy catalog bootstrap under parallel-fork test load — the unit-level
 * {@link org.apache.fluss.kafka.sr.auth.HttpPrincipalExtractorTest} covers the extractor's own
 * behaviour; this IT proves the extraction is wired through the Netty handler into the service.
 */
class SchemaRegistryHttpAuthITCase {

    private static final String KAFKA_LISTENER = "KAFKA";
    private static final String KAFKA_DATABASE = "kafka";

    private static final String ALICE = "alice";
    private static final String ALICE_PASSWORD = "alice-secret";
    private static final String BASIC_JAAS =
            "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required "
                    + "user_"
                    + ALICE
                    + "=\""
                    + ALICE_PASSWORD
                    + "\";";

    private static final int SR_PORT = freePort();

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setCoordinatorServerListeners("FLUSS://localhost:0")
                    .setTabletServerListeners(
                            "FLUSS://localhost:0," + KAFKA_LISTENER + "://localhost:0")
                    .setClusterConf(authClusterConf())
                    .build();

    private static final HttpClient HTTP =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @BeforeAll
    static void prepareDatabase() throws Exception {
        // Ensure the Kafka database namespace exists — the SR bootstrap doesn't create it.
        CLUSTER.newCoordinatorClient()
                .createDatabase(RpcMessageTestUtils.newCreateDatabaseRequest(KAFKA_DATABASE, true))
                .get();

        // Warm up the catalog: the very first SR request that triggers authorize() creates the
        // internal __principals__ / __grants__ PK tables on-demand and blocks on a 20s
        // readiness probe. Under parallel-fork test load that initial creation can itself time
        // out, leaving the service returning 500 until the buckets settle. Poll until a real SR
        // call comes back with the expected 403 — that signals the catalog is fully serving.
        waitForCatalogToServe403();
    }

    private static void waitForCatalogToServe403() throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
        int lastStatus = -1;
        while (System.nanoTime() < deadline) {
            try {
                HttpResponse<String> resp = http("GET", "/subjects", null, Collections.emptyMap());
                lastStatus = resp.statusCode();
                if (lastStatus == 403) {
                    return;
                }
            } catch (Exception ignore) {
                // Network hiccup or client-side timeout — keep polling.
            }
            Thread.sleep(1000);
        }
        // Don't fail hard — throw so @BeforeAll fails and downstream tests are marked errored.
        // CI has more headroom than a single dev machine; if the catalog really cannot initialise,
        // that's a signal to investigate the coordinator, not a reason to skip.
        throw new IllegalStateException(
                "SR catalog bootstrap never settled; last status=" + lastStatus);
    }

    @Test
    void anonymousRequestIsForbiddenAndNamedAsAnonymous() throws Exception {
        HttpResponse<String> resp = http("GET", "/subjects", null, Collections.emptyMap());
        assertThat(resp.statusCode()).isEqualTo(403);
        assertThat(resp.body()).contains("'ANONYMOUS'");
    }

    @Test
    void forwardedUserFromTrustedCidrPropagatesPrincipal() throws Exception {
        HttpResponse<String> resp =
                http("GET", "/subjects", null, Collections.singletonMap("X-Forwarded-User", ALICE));
        // Still 403 (no grant), but the error message proves alice reached the service.
        assertThat(resp.statusCode()).isEqualTo(403);
        assertThat(resp.body()).contains("'" + ALICE + "'");
    }

    @Test
    void basicAuthWithCorrectPasswordPropagatesPrincipal() throws Exception {
        HttpResponse<String> resp =
                http(
                        "GET",
                        "/subjects",
                        null,
                        Collections.singletonMap(
                                "Authorization", basicHeader(ALICE, ALICE_PASSWORD)));
        assertThat(resp.statusCode()).isEqualTo(403);
        assertThat(resp.body()).contains("'" + ALICE + "'");
    }

    @Test
    void basicAuthWithWrongPasswordFallsBackToAnonymous() throws Exception {
        HttpResponse<String> resp =
                http(
                        "GET",
                        "/subjects",
                        null,
                        Collections.singletonMap(
                                "Authorization", basicHeader(ALICE, "not-the-password")));
        assertThat(resp.statusCode()).isEqualTo(403);
        assertThat(resp.body()).contains("'ANONYMOUS'");
    }

    @Test
    void forwardedUserBeatsBasicWhenBothPresent() throws Exception {
        // If the header path were ignored, wrong Basic creds would fall back to ANONYMOUS.
        // Test that X-Forwarded-User wins and the extractor hands alice to the service.
        java.util.Map<String, String> both = new java.util.HashMap<>();
        both.put("X-Forwarded-User", ALICE);
        both.put("Authorization", basicHeader(ALICE, "wrong"));
        HttpResponse<String> resp = http("GET", "/subjects", null, both);
        assertThat(resp.statusCode()).isEqualTo(403);
        assertThat(resp.body()).contains("'" + ALICE + "'");
    }

    private static String basicHeader(String user, String password) {
        String raw = user + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    private static HttpResponse<String> http(
            String method, String path, String body, java.util.Map<String, String> headers)
            throws Exception {
        HttpRequest.Builder req =
                HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + SR_PORT + path))
                        .timeout(Duration.ofSeconds(30))
                        .header("Content-Type", "application/vnd.schemaregistry.v1+json");
        for (java.util.Map.Entry<String, String> h : headers.entrySet()) {
            req.header(h.getKey(), h.getValue());
        }
        if ("POST".equals(method)) {
            req.POST(
                    HttpRequest.BodyPublishers.ofString(
                            body == null ? "" : body, StandardCharsets.UTF_8));
        } else {
            req.GET();
        }
        return HTTP.send(req.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    private static int freePort() {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Configuration authClusterConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KAFKA_ENABLED, true);
        conf.setString(ConfigOptions.KAFKA_LISTENER_NAMES.key(), KAFKA_LISTENER);
        conf.set(ConfigOptions.KAFKA_DATABASE, KAFKA_DATABASE);
        conf.set(ConfigOptions.KAFKA_SCHEMA_REGISTRY_ENABLED, true);
        conf.set(ConfigOptions.KAFKA_SCHEMA_REGISTRY_HOST, "127.0.0.1");
        conf.set(ConfigOptions.KAFKA_SCHEMA_REGISTRY_PORT, SR_PORT);
        conf.set(ConfigOptions.KAFKA_SCHEMA_REGISTRY_RBAC_ENFORCED, true);
        conf.set(
                ConfigOptions.KAFKA_SCHEMA_REGISTRY_TRUSTED_PROXY_CIDRS,
                Arrays.asList("127.0.0.0/8", "::1/128"));
        conf.set(ConfigOptions.KAFKA_SCHEMA_REGISTRY_BASIC_AUTH_JAAS_CONFIG, BASIC_JAAS);
        return conf;
    }
}
