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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.objectstore.AbstractObjectStoreFileSystem;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/**
 * A {@link org.apache.fluss.fs.FileSystem} for Azure Blob Storage built directly on the
 * azure-storage-blob SDK, with no Hadoop dependency. The hierarchical directory emulation lives in
 * {@link AbstractObjectStoreFileSystem}; this class only implements the Azure Blob primitive
 * operations.
 *
 * <p>The Fluss "bucket" (URI authority) is mapped to an Azure Blob container; object keys are blob
 * names within that container.
 */
@Internal
public class AzureNativeFileSystem extends AbstractObjectStoreFileSystem {

    private final BlobServiceClient serviceClient;
    private final Map<String, String> tokenAdditionInfos;
    private final String scheme;

    AzureNativeFileSystem(
            String scheme,
            URI fsUri,
            BlobServiceClient serviceClient,
            Map<String, String> tokenAdditionInfos) {
        super(scheme, fsUri);
        this.scheme = scheme;
        this.serviceClient = serviceClient;
        this.tokenAdditionInfos = tokenAdditionInfos;
    }

    @Override
    public ObtainedSecurityToken obtainSecurityToken() throws IOException {
        // v1: the connection details (endpoint/account/key) are resolved per node from the Fluss
        // configuration, so no token payload is shipped here; the addition-info map carries the
        // non-secret connection hints only.
        return new ObtainedSecurityToken(scheme, new byte[0], null, tokenAdditionInfos);
    }

    @Override
    public FSDataInputStream open(FsPath f) throws IOException {
        return new AzureNativeDataInputStream(blobClient(bucketOf(f), keyOf(f)));
    }

    @Override
    public FSDataOutputStream create(FsPath f, WriteMode overwriteMode) throws IOException {
        String bucket = bucketOf(f);
        String key = keyOf(f);
        if (overwriteMode == WriteMode.NO_OVERWRITE && headLength(bucket, key).isPresent()) {
            throw new IOException("File already exists: " + f);
        }
        return new AzureNativeDataOutputStream(blobClient(bucket, key));
    }

    // ------------------------------------------------------------------------
    //  object-store primitives
    // ------------------------------------------------------------------------

    @Override
    protected OptionalLong headLength(String bucket, String key) throws IOException {
        if (key.isEmpty()) {
            return OptionalLong.empty();
        }
        try {
            return OptionalLong.of(blobClient(bucket, key).getProperties().getBlobSize());
        } catch (BlobStorageException e) {
            if (e.getStatusCode() == 404) {
                return OptionalLong.empty();
            }
            throw new IOException("Failed to HEAD " + uriOf(bucket, key), e);
        }
    }

    @Override
    protected boolean hasChildrenUnder(String bucket, String prefix) throws IOException {
        try {
            ListBlobsOptions options =
                    new ListBlobsOptions().setPrefix(prefix).setMaxResultsPerPage(1);
            return containerClient(bucket).listBlobs(options, null).iterator().hasNext();
        } catch (BlobStorageException e) {
            throw new IOException("Failed to list " + uriOf(bucket, prefix), e);
        }
    }

    @Override
    protected List<ObjectEntry> listImmediateChildren(String bucket, String dirPrefix)
            throws IOException {
        List<ObjectEntry> results = new ArrayList<>();
        try {
            ListBlobsOptions options = new ListBlobsOptions().setPrefix(dirPrefix);
            for (BlobItem item :
                    containerClient(bucket)
                            .listBlobsByHierarchy(DIR_MARKER_SUFFIX, options, null)) {
                if (Boolean.TRUE.equals(item.isPrefix())) {
                    results.add(ObjectEntry.dir(item.getName()));
                } else {
                    long size =
                            item.getProperties() != null
                                            && item.getProperties().getContentLength() != null
                                    ? item.getProperties().getContentLength()
                                    : 0L;
                    results.add(ObjectEntry.file(item.getName(), size));
                }
            }
        } catch (BlobStorageException e) {
            throw new IOException("Failed to list " + uriOf(bucket, dirPrefix), e);
        }
        return results;
    }

    @Override
    protected List<String> listAllKeys(String bucket, String prefix) throws IOException {
        List<String> keys = new ArrayList<>();
        try {
            ListBlobsOptions options = new ListBlobsOptions().setPrefix(prefix);
            for (BlobItem item : containerClient(bucket).listBlobs(options, null)) {
                keys.add(item.getName());
            }
        } catch (BlobStorageException e) {
            throw new IOException("Failed to list " + uriOf(bucket, prefix), e);
        }
        return keys;
    }

    @Override
    protected void putEmptyObject(String bucket, String key) throws IOException {
        try {
            blobClient(bucket, key)
                    .getBlockBlobClient()
                    .upload(BinaryData.fromBytes(new byte[0]), true);
        } catch (BlobStorageException e) {
            throw new IOException("Failed to write " + uriOf(bucket, key), e);
        }
    }

    @Override
    protected void deleteObject(String bucket, String key) throws IOException {
        try {
            blobClient(bucket, key).deleteIfExists();
        } catch (BlobStorageException e) {
            throw new IOException("Failed to delete " + uriOf(bucket, key), e);
        }
    }

    @Override
    protected void copyObject(String srcBucket, String srcKey, String dstBucket, String dstKey)
            throws IOException {
        try {
            BlobClient src = blobClient(srcBucket, srcKey);
            blobClient(dstBucket, dstKey).copyFromUrl(src.getBlobUrl());
        } catch (BlobStorageException e) {
            throw new IOException(
                    "Failed to copy "
                            + uriOf(srcBucket, srcKey)
                            + " to "
                            + uriOf(dstBucket, dstKey),
                    e);
        }
    }

    // ------------------------------------------------------------------------
    //  helpers
    // ------------------------------------------------------------------------

    private BlobContainerClient containerClient(String bucket) {
        return serviceClient.getBlobContainerClient(bucket);
    }

    private BlobClient blobClient(String bucket, String key) {
        return containerClient(bucket).getBlobClient(key);
    }

    private static String uriOf(String bucket, String key) {
        return "azure://" + bucket + "/" + key;
    }

    /**
     * Builds the additional-info map carried in the security token (the non-secret connection
     * hints).
     */
    static Map<String, String> tokenAdditionInfos(Map<String, String> azureSettings) {
        Map<String, String> infos = new java.util.LinkedHashMap<>();
        for (String k :
                new String[] {
                    AzureNativeSettings.ENDPOINT,
                    AzureNativeSettings.ACCOUNT_NAME,
                    AzureNativeSettings.BLOB_ENDPOINT
                }) {
            String v = azureSettings.get(k);
            if (v != null) {
                infos.put(k, v);
            }
        }
        return infos;
    }
}
