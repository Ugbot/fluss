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

package org.apache.fluss.fs.s3native;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FileSystemBehaviorTestSuite;
import org.apache.fluss.fs.FsPath;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.MinIOContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;
import java.util.UUID;

/**
 * An implementation of the {@link FileSystemBehaviorTestSuite} for the native (Hadoop-free) S3 file
 * system, run against a MinIO container so it needs no real AWS credentials.
 */
class S3NativeFileSystemBehaviorITCase extends FileSystemBehaviorTestSuite {

    private static final String BUCKET = "fluss-test-bucket";
    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    private static final MinIOContainer MINIO =
            new MinIOContainer("minio/minio:RELEASE.2024-06-29T01-20-47Z");

    @BeforeAll
    static void setup() {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                org.testcontainers.DockerClientFactory.instance().isDockerAvailable(),
                "Docker/podman is not available; skipping the container-backed behavior suite.");
        MINIO.start();

        try (S3Client s3 =
                S3Client.builder()
                        .endpointOverride(URI.create(MINIO.getS3URL()))
                        .region(Region.US_EAST_1)
                        .credentialsProvider(
                                StaticCredentialsProvider.create(
                                        AwsBasicCredentials.create(
                                                MINIO.getUserName(), MINIO.getPassword())))
                        .serviceConfiguration(
                                S3Configuration.builder().pathStyleAccessEnabled(true).build())
                        .httpClientBuilder(ApacheHttpClient.builder())
                        .build()) {
            s3.createBucket(b -> b.bucket(BUCKET));
        }

        final Configuration conf = new Configuration();
        conf.setString("s3.endpoint", MINIO.getS3URL());
        conf.setString("s3.region", "us-east-1");
        conf.setString("s3.access.key", MINIO.getUserName());
        conf.setString("s3.secret.key", MINIO.getPassword());
        conf.setString("s3.path.style.access", "true");
        FileSystem.initialize(conf, null);
    }

    @AfterAll
    static void teardown() {
        MINIO.stop();
    }

    @Override
    protected FileSystem getFileSystem() throws Exception {
        return getBasePath().getFileSystem();
    }

    @Override
    protected FsPath getBasePath() {
        return new FsPath("s3://" + BUCKET + "/" + TEST_DATA_DIR);
    }
}
