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

import org.apache.fluss.config.ConfigBuilder;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FileSystemPlugin;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Base factory for the native (Hadoop-free) Azure Blob Storage file system. Concrete subclasses
 * bind the {@code abfs}, {@code abfss}, {@code wasb} and {@code wasbs} schemes.
 *
 * <p>The Fluss "bucket" (URI authority) is mapped to an Azure Blob container.
 *
 * <p>Configuration uses the prefixes {@code azure.} and {@code fs.azure.}; within a prefix both
 * dotted and hyphenated forms are accepted (e.g. {@code azure.account.name} or {@code
 * azure.account-name}). Recognized settings: {@code connection.string}, {@code endpoint} (alias
 * {@code blob.endpoint}), {@code account.name}, {@code account.key}.
 */
abstract class AbstractAzureNativeFileSystemPlugin implements FileSystemPlugin {

    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractAzureNativeFileSystemPlugin.class);

    private static final String[] CONFIG_PREFIXES = {"azure.", "fs.azure."};

    @Override
    public FileSystem create(URI fsUri, Configuration flussConfig) throws IOException {
        Map<String, String> azure = extractAzureSettings(flussConfig);

        BlobServiceClient serviceClient = buildServiceClient(azure);

        LOG.info(
                "Created native Azure Blob file system for scheme '{}' (endpoint={}, account={}, creds={}).",
                getScheme(),
                endpointOf(azure),
                azure.getOrDefault(AzureNativeSettings.ACCOUNT_NAME, "<default>"),
                azure.get(AzureNativeSettings.CONNECTION_STRING) != null
                        ? "connection-string"
                        : (azure.get(AzureNativeSettings.ACCOUNT_KEY) != null
                                ? "shared-key"
                                : "default-chain"));

        return new AzureNativeFileSystem(
                getScheme(), fsUri, serviceClient, AzureNativeFileSystem.tokenAdditionInfos(azure));
    }

    private BlobServiceClient buildServiceClient(Map<String, String> azure) throws IOException {
        BlobServiceClientBuilder builder = new BlobServiceClientBuilder();

        String connectionString = azure.get(AzureNativeSettings.CONNECTION_STRING);
        if (connectionString != null) {
            return builder.connectionString(connectionString).buildClient();
        }

        String endpoint = endpointOf(azure);
        if (endpoint == null) {
            throw new IOException(
                    "Native Azure Blob file system requires either '"
                            + AzureNativeSettings.CONNECTION_STRING
                            + "' or '"
                            + AzureNativeSettings.ENDPOINT
                            + "' to be configured (prefix 'azure.' or 'fs.azure.').");
        }
        builder.endpoint(endpoint);

        String accountName = azure.get(AzureNativeSettings.ACCOUNT_NAME);
        String accountKey = azure.get(AzureNativeSettings.ACCOUNT_KEY);
        if (accountName != null && accountKey != null) {
            builder.credential(new StorageSharedKeyCredential(accountName, accountKey));
        }
        return builder.buildClient();
    }

    private static String endpointOf(Map<String, String> azure) {
        String endpoint = azure.get(AzureNativeSettings.ENDPOINT);
        if (endpoint == null) {
            endpoint = azure.get(AzureNativeSettings.BLOB_ENDPOINT);
        }
        return endpoint;
    }

    private static Map<String, String> extractAzureSettings(Configuration flussConfig) {
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
