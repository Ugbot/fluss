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

package org.apache.fluss.fs.gsnative;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FileSystemBehaviorTestSuite;
import org.apache.fluss.fs.FsPath;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * An implementation of the {@link FileSystemBehaviorTestSuite} for the native (Hadoop-free) Google
 * Cloud Storage file system, run against a fake-gcs-server container so it needs no real GCP
 * credentials.
 */
class GsNativeFileSystemBehaviorITCase extends FileSystemBehaviorTestSuite {

    private static final String BUCKET = "fluss-test-bucket";
    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();
    private static final int GCS_PORT = 4443;

    private static final GenericContainer<?> FAKE_GCS =
            new GenericContainer<>(DockerImageName.parse("fsouza/fake-gcs-server:latest"))
                    .withExposedPorts(GCS_PORT)
                    // The default filesystem backend maps each key to a real file on disk and so
                    // cannot store the zero-byte "dir/" marker objects that the directory emulation
                    // relies on (a key ending in "/" is silently dropped). The in-memory backend
                    // preserves such keys, matching real GCS semantics.
                    .withCommand(
                            "-scheme",
                            "http",
                            "-port",
                            String.valueOf(GCS_PORT),
                            "-backend",
                            "memory")
                    .waitingFor(Wait.forListeningPort());

    private static String endpoint;

    @BeforeAll
    static void setup() throws Exception {
        FAKE_GCS.start();

        endpoint = "http://" + FAKE_GCS.getHost() + ":" + FAKE_GCS.getMappedPort(GCS_PORT);

        // fake-gcs-server hands out object download/upload URLs using its own configured
        // "external URL"; by default that is the in-container address, which the SDK on the host
        // cannot reach. Reconfigure it to the host-visible mapped address.
        updateExternalUrl(endpoint);

        try (Storage storage =
                StorageOptions.newBuilder()
                        .setHost(endpoint)
                        .setProjectId("test")
                        .setCredentials(NoCredentials.getInstance())
                        .build()
                        .getService()) {
            storage.create(BucketInfo.newBuilder(BUCKET).build());
        }

        final Configuration conf = new Configuration();
        conf.setString("gs.endpoint", endpoint);
        conf.setString("gs.project.id", "test");
        FileSystem.initialize(conf, null);
    }

    private static void updateExternalUrl(String externalUrl) throws IOException {
        URL url = new URL(endpoint + "/_internal/config");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        try {
            connection.setRequestMethod("PUT");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setDoOutput(true);
            byte[] payload =
                    ("{\"externalUrl\":\"" + externalUrl + "\"}").getBytes(StandardCharsets.UTF_8);
            connection.getOutputStream().write(payload);
            int code = connection.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK) {
                throw new IOException("Failed to set fake-gcs-server external URL, code=" + code);
            }
        } finally {
            connection.disconnect();
        }
    }

    @AfterAll
    static void teardown() {
        FAKE_GCS.stop();
    }

    @Override
    protected FileSystem getFileSystem() throws Exception {
        return getBasePath().getFileSystem();
    }

    @Override
    protected FsPath getBasePath() {
        return new FsPath("gs://" + BUCKET + "/" + TEST_DATA_DIR);
    }
}
