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

import org.apache.fluss.config.ConfigBuilder;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FileSystemPlugin;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Base factory for the native (Hadoop-free) Google Cloud Storage file system. The concrete subclass
 * binds the {@code gs} scheme.
 *
 * <p>Configuration uses the {@code gs.} and {@code fs.gs.} prefixes; within a prefix both dotted
 * and hyphenated forms are accepted (e.g. {@code gs.project.id} or {@code gs.project-id}).
 * Recognized settings: {@code endpoint} (a custom GCS endpoint, e.g. an emulator), {@code
 * project.id}, {@code credentials.json} (path to a service-account key file). When no credentials
 * are configured, the Application Default Credentials chain is used; when an {@code endpoint} is
 * set without credentials, anonymous (no-credentials) access is used so the emulator works out of
 * the box.
 */
abstract class AbstractGsNativeFileSystemPlugin implements FileSystemPlugin {

    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractGsNativeFileSystemPlugin.class);

    private static final String[] CONFIG_PREFIXES = {"gs.", "fs.gs."};

    private static final String ENDPOINT = "endpoint";
    private static final String PROJECT_ID = "project.id";
    private static final String CREDENTIALS_JSON = "credentials.json";

    private static final String DEFAULT_PROJECT_ID = "fluss";

    @Override
    public FileSystem create(URI fsUri, Configuration flussConfig) throws IOException {
        Map<String, String> gs = extractGsSettings(flussConfig);

        StorageOptions.Builder builder = StorageOptions.newBuilder();

        String projectId = gs.getOrDefault(PROJECT_ID, DEFAULT_PROJECT_ID);
        builder.setProjectId(projectId);

        String endpoint = gs.get(ENDPOINT);
        if (endpoint != null) {
            builder.setHost(endpoint);
        }

        Credentials credentials = resolveCredentials(gs, endpoint);
        if (credentials != null) {
            builder.setCredentials(credentials);
        }

        Storage storage = builder.build().getService();

        LOG.info(
                "Created native GS file system for scheme '{}' (endpoint={}, projectId={}, creds={}).",
                getScheme(),
                endpoint == null ? "<default>" : endpoint,
                projectId,
                credentials instanceof NoCredentials
                        ? "anonymous"
                        : (gs.get(CREDENTIALS_JSON) != null ? "service-account" : "default-chain"));

        return new GsNativeFileSystem(
                getScheme(), fsUri, storage, GsNativeFileSystem.tokenAdditionInfos(gs));
    }

    private Credentials resolveCredentials(Map<String, String> gs, String endpoint)
            throws IOException {
        String credentialsJson = gs.get(CREDENTIALS_JSON);
        if (credentialsJson != null) {
            try (InputStream in = Files.newInputStream(Paths.get(credentialsJson))) {
                return GoogleCredentials.fromStream(in);
            }
        }
        if (endpoint != null) {
            // A custom endpoint without explicit credentials is the emulator case; use anonymous
            // access so no real GCP credential resolution is attempted.
            return NoCredentials.getInstance();
        }
        // No explicit credentials against the real service: fall back to Application Default
        // Credentials (returning null lets StorageOptions resolve the default chain itself).
        return null;
    }

    private static Map<String, String> extractGsSettings(Configuration flussConfig) {
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
