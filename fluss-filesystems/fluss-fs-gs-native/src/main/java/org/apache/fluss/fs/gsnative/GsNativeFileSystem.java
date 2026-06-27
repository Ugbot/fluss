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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.objectstore.AbstractObjectStoreFileSystem;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/**
 * A {@link org.apache.fluss.fs.FileSystem} for Google Cloud Storage (and GCS-compatible stores such
 * as the fake-gcs-server emulator) built directly on the google-cloud-storage SDK, with no Hadoop
 * dependency. The hierarchical directory emulation lives in {@link AbstractObjectStoreFileSystem};
 * this class only implements the GCS primitive operations.
 */
@Internal
public class GsNativeFileSystem extends AbstractObjectStoreFileSystem {

    private final Storage storage;
    private final Map<String, String> tokenAdditionInfos;
    private final String scheme;

    GsNativeFileSystem(
            String scheme, URI fsUri, Storage storage, Map<String, String> tokenAdditionInfos) {
        super(scheme, fsUri);
        this.scheme = scheme;
        this.storage = storage;
        this.tokenAdditionInfos = tokenAdditionInfos;
    }

    @Override
    public ObtainedSecurityToken obtainSecurityToken() throws IOException {
        // v1: no token is vended; each node resolves credentials from its own configuration /
        // Application Default Credentials chain. The endpoint/project metadata is still carried so
        // clients target the same store.
        return new ObtainedSecurityToken(scheme, new byte[0], null, tokenAdditionInfos);
    }

    @Override
    public FSDataInputStream open(FsPath f) throws IOException {
        return new GsDataInputStream(storage, bucketOf(f), normalizeKey(keyOf(f)));
    }

    @Override
    public FSDataOutputStream create(FsPath f, WriteMode overwriteMode) throws IOException {
        String bucket = bucketOf(f);
        String key = normalizeKey(keyOf(f));
        if (overwriteMode == WriteMode.NO_OVERWRITE && headLength(bucket, key).isPresent()) {
            throw new IOException("File already exists: " + f);
        }
        return new GsDataOutputStream(storage, bucket, key);
    }

    // ------------------------------------------------------------------------
    //  object-store primitives
    // ------------------------------------------------------------------------

    @Override
    protected OptionalLong headLength(String bucket, String key) throws IOException {
        String normalized = normalizeKey(key);
        if (normalized.isEmpty()) {
            return OptionalLong.empty();
        }
        // A key ending in "/" denotes an (emulated) directory marker, never a file. Reporting it as
        // a file would make getFileStatus/mkdirs misclassify trailing-slash directory paths.
        if (normalized.endsWith(DIR_MARKER_SUFFIX)) {
            return OptionalLong.empty();
        }
        try {
            Blob blob = storage.get(BlobId.of(bucket, normalized));
            if (blob == null || !blob.exists()) {
                return OptionalLong.empty();
            }
            Long size = blob.getSize();
            return OptionalLong.of(size == null ? 0L : size);
        } catch (StorageException e) {
            if (e.getCode() == 404) {
                return OptionalLong.empty();
            }
            throw new IOException("Failed to HEAD gs://" + bucket + "/" + normalized, e);
        }
    }

    @Override
    protected boolean hasChildrenUnder(String bucket, String prefix) throws IOException {
        String normalized = normalizeKey(prefix);
        try {
            for (Blob ignored :
                    storage.list(
                                    bucket,
                                    Storage.BlobListOption.prefix(normalized),
                                    Storage.BlobListOption.pageSize(1))
                            .iterateAll()) {
                return true;
            }
            return false;
        } catch (StorageException e) {
            throw new IOException("Failed to list gs://" + bucket + "/" + normalized, e);
        }
    }

    @Override
    protected List<ObjectEntry> listImmediateChildren(String bucket, String dirPrefix)
            throws IOException {
        String normalized = normalizeKey(dirPrefix);
        List<ObjectEntry> results = new ArrayList<>();
        try {
            for (Blob blob :
                    storage.list(
                                    bucket,
                                    Storage.BlobListOption.prefix(normalized),
                                    Storage.BlobListOption.currentDirectory())
                            .iterateAll()) {
                if (blob.isDirectory()) {
                    // a synthetic directory entry from the "/" delimiter; its name ends in "/"
                    results.add(
                            ObjectEntry.dir(restoreSelf(blob.getName(), dirPrefix, normalized)));
                } else {
                    Long size = blob.getSize();
                    results.add(
                            ObjectEntry.file(
                                    restoreSelf(blob.getName(), dirPrefix, normalized),
                                    size == null ? 0L : size));
                }
            }
        } catch (StorageException e) {
            throw new IOException("Failed to list gs://" + bucket + "/" + normalized, e);
        }
        return results;
    }

    @Override
    protected List<String> listAllKeys(String bucket, String prefix) throws IOException {
        String normalized = normalizeKey(prefix);
        List<String> keys = new ArrayList<>();
        try {
            for (Blob blob :
                    storage.list(bucket, Storage.BlobListOption.prefix(normalized)).iterateAll()) {
                keys.add(restoreSelf(blob.getName(), prefix, normalized));
            }
        } catch (StorageException e) {
            throw new IOException("Failed to list gs://" + bucket + "/" + normalized, e);
        }
        return keys;
    }

    @Override
    protected void putEmptyObject(String bucket, String key) throws IOException {
        String normalized = normalizeKey(key);
        try {
            storage.create(BlobInfo.newBuilder(BlobId.of(bucket, normalized)).build(), new byte[0]);
        } catch (StorageException e) {
            throw new IOException("Failed to write gs://" + bucket + "/" + normalized, e);
        }
    }

    @Override
    protected void deleteObject(String bucket, String key) throws IOException {
        String normalized = normalizeKey(key);
        try {
            // deleting a non-existent key returns false rather than throwing; that is fine.
            storage.delete(BlobId.of(bucket, normalized));
        } catch (StorageException e) {
            throw new IOException("Failed to delete gs://" + bucket + "/" + normalized, e);
        }
    }

    @Override
    protected void copyObject(String srcBucket, String srcKey, String dstBucket, String dstKey)
            throws IOException {
        String srcNormalized = normalizeKey(srcKey);
        String dstNormalized = normalizeKey(dstKey);
        try {
            Storage.CopyRequest copyRequest =
                    Storage.CopyRequest.of(
                            BlobId.of(srcBucket, srcNormalized),
                            BlobInfo.newBuilder(BlobId.of(dstBucket, dstNormalized)).build());
            // getResult() blocks until the (possibly chunked) copy completes.
            storage.copy(copyRequest).getResult();
        } catch (StorageException e) {
            throw new IOException(
                    "Failed to copy gs://"
                            + srcBucket
                            + "/"
                            + srcNormalized
                            + " to gs://"
                            + dstBucket
                            + "/"
                            + dstNormalized,
                    e);
        }
    }

    /**
     * Collapses runs of consecutive {@code "/"} into a single slash. The shared directory emulation
     * always appends exactly one {@link #DIR_MARKER_SUFFIX}, which produces a double slash whenever
     * the caller supplied a path that already ended in {@code "/"} (e.g. {@code mkdirs("dir/")}).
     * Google Cloud Storage (and the fake-gcs-server emulator) silently normalize or reject keys
     * containing {@code "//"}, so we normalize here to keep written and queried keys consistent.
     */
    private static String normalizeKey(String key) {
        if (key.indexOf("//") < 0) {
            return key;
        }
        StringBuilder sb = new StringBuilder(key.length());
        boolean prevSlash = false;
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            if (c == '/') {
                if (prevSlash) {
                    continue;
                }
                prevSlash = true;
            } else {
                prevSlash = false;
            }
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * Maps a key returned by GCS back to the exact spelling the shared layer expects. When the
     * requested prefix contained redundant slashes, GCS returns the normalized form; the base class
     * compares a directory's own marker against the un-normalized prefix it passed in, so a
     * returned key that equals the normalized prefix is rewritten back to that original prefix.
     */
    private static String restoreSelf(
            String returnedKey, String originalPrefix, String normalized) {
        return returnedKey.equals(normalized) ? originalPrefix : returnedKey;
    }

    /** Builds the additional-info map carried in the security token (endpoint/project). */
    static Map<String, String> tokenAdditionInfos(Map<String, String> gsSettings) {
        Map<String, String> infos = new java.util.LinkedHashMap<>();
        for (String k : new String[] {"endpoint", "project.id"}) {
            String v = gsSettings.get(k);
            if (v != null) {
                infos.put(k, v);
            }
        }
        return infos;
    }
}
