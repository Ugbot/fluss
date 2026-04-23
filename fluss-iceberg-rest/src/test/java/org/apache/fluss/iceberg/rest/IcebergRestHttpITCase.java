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

package org.apache.fluss.iceberg.rest;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end IT for the Iceberg REST Catalog HTTP listener. Verifies that the endpoint is stood up
 * on the coordinator leader, that the three Phase E preview routes are wired correctly, and that
 * they are a real projection over the shared catalog — POST-then-GET round-trips through the same
 * {@code FlussCatalogService} instance used by the Kafka Schema Registry.
 */
class IcebergRestHttpITCase {

    private static final int REST_PORT = freePort();

    @RegisterExtension
    static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setCoordinatorServerListeners("FLUSS://localhost:0")
                    .setTabletServerListeners("FLUSS://localhost:0")
                    .setClusterConf(restClusterConf())
                    .build();

    private static final HttpClient HTTP =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void getConfigReturnsEmptyDefaultsAndOverrides() throws Exception {
        HttpResponse<String> resp = http("GET", "/v1/config", null);
        assertThat(resp.statusCode()).isEqualTo(200);
        JsonNode body = MAPPER.readTree(resp.body());
        assertThat(body.has("defaults")).isTrue();
        assertThat(body.has("overrides")).isTrue();
        assertThat(body.get("defaults").isObject()).isTrue();
        assertThat(body.get("overrides").isObject()).isTrue();
        assertThat(body.get("defaults").size()).isEqualTo(0);
        assertThat(body.get("overrides").size()).isEqualTo(0);
    }

    @Test
    void createAndListNamespaces() throws Exception {
        String name = "ns_" + System.nanoTime();
        String postBody = "{\"namespace\":[\"" + name + "\"],\"properties\":{}}";

        // First POST may race the catalog's lazy system-table provisioning; retry on
        // 500 with the well-known "did not become ready" readiness-window cause. Once the
        // NAMESPACES system table is fully registered and bucket-leader elected, subsequent
        // requests are fast.
        HttpResponse<String> created = postWithReadinessRetry("/v1/namespaces", postBody);
        assertThat(created.statusCode()).isEqualTo(201);
        JsonNode createdBody = MAPPER.readTree(created.body());
        assertThat(createdBody.get("namespace").isArray()).isTrue();
        assertThat(createdBody.get("namespace").size()).isEqualTo(1);
        assertThat(createdBody.get("namespace").get(0).asText()).isEqualTo(name);

        HttpResponse<String> listed = http("GET", "/v1/namespaces", null);
        assertThat(listed.statusCode()).isEqualTo(200);
        JsonNode listedBody = MAPPER.readTree(listed.body());
        assertThat(listedBody.get("namespaces").isArray()).isTrue();
        boolean found = false;
        for (JsonNode levels : listedBody.get("namespaces")) {
            assertThat(levels.isArray()).isTrue();
            if (levels.size() == 1 && name.equals(levels.get(0).asText())) {
                found = true;
                break;
            }
        }
        assertThat(found).as("GET /v1/namespaces should contain the created namespace").isTrue();
    }

    @Test
    void duplicateNamespaceReturns409() throws Exception {
        String name = "dup_" + System.nanoTime();
        String postBody = "{\"namespace\":[\"" + name + "\"],\"properties\":{}}";

        HttpResponse<String> first = postWithReadinessRetry("/v1/namespaces", postBody);
        assertThat(first.statusCode()).isEqualTo(201);

        HttpResponse<String> second = http("POST", "/v1/namespaces", postBody);
        assertThat(second.statusCode()).isEqualTo(409);
        JsonNode body = MAPPER.readTree(second.body());
        assertThat(body.has("error")).isTrue();
        assertThat(body.get("error").get("code").asInt()).isEqualTo(409);
    }

    @Test
    void unimplementedRouteReturns501() throws Exception {
        HttpResponse<String> resp = http("GET", "/v1/namespaces/foo/tables", null);
        assertThat(resp.statusCode()).isEqualTo(501);
        JsonNode body = MAPPER.readTree(resp.body());
        assertThat(body.get("error").get("code").asInt()).isEqualTo(501);
    }

    private static HttpResponse<String> postWithReadinessRetry(String path, String body)
            throws Exception {
        // The catalog's system-table provisioning (createTable + wait for bucket leadership +
        // TabletServer metadata propagation) can take more than the catalog's 20s readiness
        // budget on a cold single-TS cluster. Retry for up to 2 minutes whenever the server
        // reports a 5xx so the test is robust against cluster warm-up jitter; a real 4xx
        // response (validation, conflict, etc.) is returned immediately.
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
        HttpResponse<String> last = null;
        while (System.currentTimeMillis() < deadline) {
            last = http("POST", path, body);
            if (last.statusCode() < 500) {
                return last;
            }
            Thread.sleep(500);
        }
        return last;
    }

    private static HttpResponse<String> http(String method, String path, String body)
            throws Exception {
        // First request triggers lazy system-table provisioning in FlussCatalogService
        // (createTable + bucket-leadership wait). Budget generously so we don't race the
        // catalog's own 20s internal readiness deadline.
        HttpRequest.Builder req =
                HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + REST_PORT + path))
                        .timeout(Duration.ofSeconds(60))
                        .header("Content-Type", "application/json");
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

    private static Configuration restClusterConf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.ICEBERG_REST_ENABLED, true);
        conf.set(ConfigOptions.ICEBERG_REST_HOST, "127.0.0.1");
        conf.set(ConfigOptions.ICEBERG_REST_PORT, REST_PORT);
        return conf;
    }
}
