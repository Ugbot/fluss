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

import org.apache.fluss.config.ConfigBuilder;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FileSystemPlugin;
import org.apache.fluss.fs.token.Credentials;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Base factory for the native (Hadoop-free) S3 file system. Concrete subclasses bind the {@code s3}
 * and {@code s3a} schemes.
 *
 * <p>Configuration uses the same prefixes as the Hadoop-based plugin ({@code s3.}, {@code s3a.},
 * {@code fs.s3a.}); within a prefix both dotted and hyphenated forms are accepted (e.g. {@code
 * s3.access.key} or {@code s3.access-key}). Recognized settings: {@code access.key}, {@code
 * secret.key}, {@code endpoint}, {@code region}, {@code path.style.access}, {@code
 * assumed.role.arn}.
 */
abstract class AbstractS3NativeFileSystemPlugin implements FileSystemPlugin {

    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractS3NativeFileSystemPlugin.class);

    private static final String[] CONFIG_PREFIXES = {"s3.", "s3a.", "fs.s3a."};

    private static final String ACCESS_KEY = "access.key";
    private static final String SECRET_KEY = "secret.key";
    private static final String ENDPOINT = "endpoint";
    private static final String REGION = "region";
    private static final String PATH_STYLE_ACCESS = "path.style.access";
    private static final String ASSUMED_ROLE_ARN = "assumed.role.arn";

    private static final String DEFAULT_REGION = "us-east-1";

    @Override
    public FileSystem create(URI fsUri, Configuration flussConfig) throws IOException {
        Map<String, String> s3 = extractS3Settings(flussConfig);

        Region region = Region.of(s3.getOrDefault(REGION, DEFAULT_REGION));
        AwsCredentialsProvider credentialsProvider = buildCredentialsProvider(s3, region);

        S3Configuration.Builder serviceConfig = S3Configuration.builder();
        if (Boolean.parseBoolean(s3.get(PATH_STYLE_ACCESS))) {
            serviceConfig.pathStyleAccessEnabled(true);
        }

        S3ClientBuilder builder =
                S3Client.builder()
                        .region(region)
                        .credentialsProvider(credentialsProvider)
                        .serviceConfiguration(serviceConfig.build())
                        .httpClientBuilder(ApacheHttpClient.builder())
                        // AWS SDK v2 2.30+ defaults to CRC32 flexible checksums and drops the
                        // legacy Content-MD5 header; many S3-compatible stores (MinIO, Ceph, OSS)
                        // still require Content-MD5 (e.g. for DeleteObjects). WHEN_REQUIRED
                        // restores
                        // the legacy behavior while still checksumming where the API mandates it.
                        .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                        .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED);
        if (s3.get(ENDPOINT) != null) {
            builder.endpointOverride(URI.create(s3.get(ENDPOINT)));
        }

        S3Client client = builder.build();

        Credentials staticCredentials = staticCredentials(s3);
        LOG.info(
                "Created native S3 file system for scheme '{}' (endpoint={}, region={}, pathStyle={}, creds={}).",
                getScheme(),
                s3.getOrDefault(ENDPOINT, "<default>"),
                region,
                Boolean.parseBoolean(s3.get(PATH_STYLE_ACCESS)),
                staticCredentials != null
                        ? "static"
                        : (s3.get(ASSUMED_ROLE_ARN) != null ? "assume-role" : "default-chain"));

        return new S3NativeFileSystem(
                getScheme(),
                fsUri,
                client,
                staticCredentials,
                S3NativeFileSystem.tokenAdditionInfos(s3));
    }

    private AwsCredentialsProvider buildCredentialsProvider(Map<String, String> s3, Region region) {
        String accessKey = s3.get(ACCESS_KEY);
        String secretKey = s3.get(SECRET_KEY);
        String roleArn = s3.get(ASSUMED_ROLE_ARN);

        if (accessKey != null && secretKey != null) {
            return StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey));
        }
        if (roleArn != null) {
            StsClient sts = StsClient.builder().region(region).build();
            return StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(sts)
                    .refreshRequest(
                            AssumeRoleRequest.builder()
                                    .roleArn(roleArn)
                                    .roleSessionName("fluss-fs-s3-native")
                                    .build())
                    .build();
        }
        return DefaultCredentialsProvider.create();
    }

    @Nullable
    private Credentials staticCredentials(Map<String, String> s3) {
        String accessKey = s3.get(ACCESS_KEY);
        String secretKey = s3.get(SECRET_KEY);
        if (accessKey != null && secretKey != null) {
            return new Credentials(accessKey, secretKey, null);
        }
        return null;
    }

    private static Map<String, String> extractS3Settings(Configuration flussConfig) {
        Map<String, String> out = new LinkedHashMap<>();
        if (flussConfig == null) {
            return out;
        }
        for (String key : flussConfig.keySet()) {
            for (String prefix : CONFIG_PREFIXES) {
                if (key.startsWith(prefix)) {
                    String canonical = key.substring(prefix.length()).replace('-', '.');
                    String value =
                            flussConfig.getString(
                                    ConfigBuilder.key(key).stringType().noDefaultValue(), null);
                    if (value != null) {
                        // first matching prefix wins; do not let a later prefix overwrite
                        out.putIfAbsent(canonical, value);
                    }
                    break;
                }
            }
        }
        return out;
    }
}
