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

package org.apache.fluss.fs.azurenative;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FileSystemBehaviorTestSuite;
import org.apache.fluss.fs.FsPath;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.UUID;

/**
 * An implementation of the {@link FileSystemBehaviorTestSuite} for the native (Hadoop-free) Azure
 * Blob file system, run against an Azurite container so it needs no real Azure credentials.
 */
class AzureNativeFileSystemBehaviorITCase extends FileSystemBehaviorTestSuite {

    /** The Azurite well-known development storage account name. */
    private static final String ACCOUNT_NAME = "devstoreaccount1";

    /** The Azurite well-known development storage account key (public, documented by Microsoft). */
    private static final String ACCOUNT_KEY =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    private static final int BLOB_PORT = 10000;

    private static final String CONTAINER_NAME = "fluss-test-container";
    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    private static final GenericContainer<?> AZURITE =
            new GenericContainer<>(
                            DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite:latest"))
                    .withExposedPorts(BLOB_PORT)
                    .withCommand("azurite-blob", "--blobHost", "0.0.0.0", "--skipApiVersionCheck")
                    .waitingFor(Wait.forListeningPort());

    private static String blobEndpoint;
    private static String connectionString;

    @BeforeAll
    static void setup() {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                org.testcontainers.DockerClientFactory.instance().isDockerAvailable(),
                "Docker/podman is not available; skipping the container-backed behavior suite.");
        AZURITE.start();

        String host = AZURITE.getHost();
        int port = AZURITE.getMappedPort(BLOB_PORT);
        // Path-style endpoint required by Azurite: http://host:port/<account>
        blobEndpoint = "http://" + host + ":" + port + "/" + ACCOUNT_NAME;
        connectionString =
                "DefaultEndpointsProtocol=http;"
                        + "AccountName="
                        + ACCOUNT_NAME
                        + ";AccountKey="
                        + ACCOUNT_KEY
                        + ";BlobEndpoint="
                        + blobEndpoint
                        + ";";

        BlobServiceClient serviceClient =
                new BlobServiceClientBuilder().connectionString(connectionString).buildClient();
        serviceClient.createBlobContainer(CONTAINER_NAME);

        final Configuration conf = new Configuration();
        conf.setString("azure.connection.string", connectionString);
        conf.setString("azure.endpoint", blobEndpoint);
        conf.setString("azure.account.name", ACCOUNT_NAME);
        conf.setString("azure.account.key", ACCOUNT_KEY);
        FileSystem.initialize(conf, null);
    }

    @AfterAll
    static void teardown() {
        AZURITE.stop();
    }

    @Override
    protected FileSystem getFileSystem() throws Exception {
        return getBasePath().getFileSystem();
    }

    @Override
    protected FsPath getBasePath() {
        return new FsPath("abfs://" + CONTAINER_NAME + "/" + TEST_DATA_DIR);
    }
}
