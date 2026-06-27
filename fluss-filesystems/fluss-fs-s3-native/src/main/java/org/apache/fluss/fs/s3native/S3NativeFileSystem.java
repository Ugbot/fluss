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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A {@link FileSystem} implementation for S3 (and S3-compatible stores) built directly on the AWS
 * SDK v2, with no Hadoop dependency.
 *
 * <p>S3 is a flat key/value store with no real directories. This class emulates the hierarchical
 * directory semantics that the Fluss {@link FileSystem} contract requires (see {@code
 * FileSystemBehaviorTestSuite}) by treating a key ending in {@code "/"} as a directory marker:
 *
 * <ul>
 *   <li>{@code mkdirs(p)} writes a zero-byte marker object at {@code <key>/} (after verifying no
 *       file exists at {@code p} or any ancestor), so empty directories are observable.
 *   <li>{@code getFileStatus(p)} is a file if a HEAD of {@code <key>} succeeds, a directory if any
 *       object exists under the {@code <key>/} prefix, otherwise not found.
 *   <li>{@code listStatus(p)} lists the {@code <key>/} prefix with a {@code "/"} delimiter, mapping
 *       common prefixes to sub-directories and objects (minus the self marker) to files.
 *   <li>recursive {@code delete} removes every object under {@code <key>/} (plus the marker).
 * </ul>
 */
@Internal
public class S3NativeFileSystem extends FileSystem {

    /** Suffix used to mark a key as an (emulated) directory. */
    private static final String DIR_MARKER_SUFFIX = "/";

    private final String scheme;
    private final URI fsUri;
    private final S3Client client;

    /** Static credentials, if configured, used to vend a security token to clients. */
    @Nullable private final Credentials staticCredentials;

    private final Map<String, String> tokenAdditionInfos;

    S3NativeFileSystem(
            String scheme,
            URI fsUri,
            S3Client client,
            @Nullable Credentials staticCredentials,
            Map<String, String> tokenAdditionInfos) {
        this.scheme = scheme;
        this.fsUri = fsUri;
        this.client = client;
        this.staticCredentials = staticCredentials;
        this.tokenAdditionInfos = tokenAdditionInfos;
    }

    @Override
    public URI getUri() {
        return fsUri;
    }

    @Override
    public ObtainedSecurityToken obtainSecurityToken() throws IOException {
        // v1: pass static credentials through to clients when configured; otherwise no token (each
        // node is expected to resolve credentials from its own default provider chain / instance
        // profile). STS-based delegation is a planned follow-up.
        byte[] token =
                staticCredentials == null
                        ? new byte[0]
                        : CredentialsJsonSerde.toJson(staticCredentials);
        return new ObtainedSecurityToken(scheme, token, null, tokenAdditionInfos);
    }

    @Override
    public FileStatus getFileStatus(FsPath f) throws IOException {
        String bucket = bucketOf(f);
        String key = keyOf(f);
        if (key.isEmpty()) {
            // bucket root is always a directory
            return new S3NativeFileStatus(f, 0L, true);
        }
        Long len = headObjectLength(bucket, key);
        if (len != null) {
            return new S3NativeFileStatus(f, len, false);
        }
        if (hasChildren(bucket, key + DIR_MARKER_SUFFIX)) {
            return new S3NativeFileStatus(f, 0L, true);
        }
        throw new FileNotFoundException("File " + f + " does not exist");
    }

    @Override
    public FSDataInputStream open(FsPath f) throws IOException {
        return new S3DataInputStream(client, bucketOf(f), keyOf(f));
    }

    @Override
    public FileStatus[] listStatus(FsPath f) throws IOException {
        String bucket = bucketOf(f);
        String key = keyOf(f);

        // a file lists as itself, mirroring LocalFileSystem
        if (!key.isEmpty()) {
            Long len = headObjectLength(bucket, key);
            if (len != null) {
                return new FileStatus[] {new S3NativeFileStatus(f, len, false)};
            }
        }

        String prefix = key.isEmpty() ? "" : key + DIR_MARKER_SUFFIX;
        List<FileStatus> results = new ArrayList<>();
        String continuationToken = null;
        do {
            ListObjectsV2Request.Builder req =
                    ListObjectsV2Request.builder()
                            .bucket(bucket)
                            .prefix(prefix)
                            .delimiter(DIR_MARKER_SUFFIX);
            if (continuationToken != null) {
                req.continuationToken(continuationToken);
            }
            ListObjectsV2Response resp = client.listObjectsV2(req.build());

            for (CommonPrefix cp : resp.commonPrefixes()) {
                // e.g. prefix "base/dir/" -> common prefix "base/dir/sub/" -> name "sub"
                String childKey = stripTrailingSlash(cp.prefix());
                results.add(new S3NativeFileStatus(pathOfKey(bucket, childKey), 0L, true));
            }
            for (S3Object o : resp.contents()) {
                if (o.key().equals(prefix)) {
                    // the directory's own marker, not a child
                    continue;
                }
                results.add(new S3NativeFileStatus(pathOfKey(bucket, o.key()), o.size(), false));
            }
            continuationToken =
                    Boolean.TRUE.equals(resp.isTruncated()) ? resp.nextContinuationToken() : null;
        } while (continuationToken != null);

        return results.toArray(new FileStatus[0]);
    }

    @Override
    public boolean delete(FsPath f, boolean recursive) throws IOException {
        String bucket = bucketOf(f);
        String key = keyOf(f);
        if (key.isEmpty()) {
            throw new IOException("Cannot delete the bucket root: " + f);
        }

        // case 1: a file at this exact key
        if (headObjectLength(bucket, key) != null) {
            deleteKeys(bucket, Collections.singletonList(key));
            return true;
        }

        // case 2: an (emulated) directory
        String dirPrefix = key + DIR_MARKER_SUFFIX;
        List<String> childKeys = new ArrayList<>();
        boolean hasNonMarkerChild = false;
        String continuationToken = null;
        do {
            ListObjectsV2Request.Builder req =
                    ListObjectsV2Request.builder().bucket(bucket).prefix(dirPrefix);
            if (continuationToken != null) {
                req.continuationToken(continuationToken);
            }
            ListObjectsV2Response resp = client.listObjectsV2(req.build());
            for (S3Object o : resp.contents()) {
                childKeys.add(o.key());
                if (!o.key().equals(dirPrefix)) {
                    hasNonMarkerChild = true;
                }
            }
            continuationToken =
                    Boolean.TRUE.equals(resp.isTruncated()) ? resp.nextContinuationToken() : null;
        } while (continuationToken != null);

        if (childKeys.isEmpty()) {
            // nothing exists at this path; deletion is vacuously successful
            return true;
        }
        if (!recursive && hasNonMarkerChild) {
            throw new IOException("Directory " + f + " is not empty");
        }
        deleteKeys(bucket, childKeys);
        return true;
    }

    @Override
    public boolean mkdirs(FsPath f) throws IOException {
        checkNotNull(f, "path is null");
        String bucket = bucketOf(f);
        String key = keyOf(f);
        if (key.isEmpty()) {
            // bucket root always exists
            return true;
        }

        // a file must not exist at the target path
        if (headObjectLength(bucket, key) != null) {
            throw new IOException("Cannot create directory " + f + ": a file exists at that path");
        }
        // no ancestor may be a file
        FsPath parent = f.getParent();
        while (parent != null) {
            String parentKey = keyOf(parent);
            if (parentKey.isEmpty()) {
                break;
            }
            if (headObjectLength(bucket, parentKey) != null) {
                throw new IOException(
                        "Cannot create directory " + f + ": a file exists at ancestor " + parent);
            }
            parent = parent.getParent();
        }

        // write the directory marker
        client.putObject(
                PutObjectRequest.builder().bucket(bucket).key(key + DIR_MARKER_SUFFIX).build(),
                software.amazon.awssdk.core.sync.RequestBody.empty());
        return true;
    }

    @Override
    public FSDataOutputStream create(FsPath f, WriteMode overwriteMode) throws IOException {
        checkNotNull(f, "path is null");
        String bucket = bucketOf(f);
        String key = keyOf(f);
        if (overwriteMode == WriteMode.NO_OVERWRITE && headObjectLength(bucket, key) != null) {
            throw new IOException("File already exists: " + f);
        }
        return new S3DataOutputStream(client, bucket, key);
    }

    @Override
    public boolean rename(FsPath src, FsPath dst) throws IOException {
        // Object stores have no atomic rename. Not used on any Fluss core hot path; implemented as
        // copy-then-delete over the (single) source object for correctness.
        String bucket = bucketOf(src);
        String srcKey = keyOf(src);
        Long len = headObjectLength(bucket, srcKey);
        if (len == null) {
            return false;
        }
        String dstBucket = bucketOf(dst);
        String dstKey = keyOf(dst);
        try {
            client.copyObject(
                    b ->
                            b.sourceBucket(bucket)
                                    .sourceKey(srcKey)
                                    .destinationBucket(dstBucket)
                                    .destinationKey(dstKey));
            deleteKeys(bucket, Collections.singletonList(srcKey));
            return true;
        } catch (Exception e) {
            throw new IOException("Failed to rename " + src + " to " + dst, e);
        }
    }

    // ------------------------------------------------------------------------
    //  helpers
    // ------------------------------------------------------------------------

    /** Returns the object length if a file exists at the key, or {@code null} if it does not. */
    @Nullable
    private Long headObjectLength(String bucket, String key) throws IOException {
        if (key.isEmpty()) {
            return null;
        }
        try {
            return client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build())
                    .contentLength();
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return null;
            }
            throw new IOException("Failed to HEAD s3://" + bucket + "/" + key, e);
        }
    }

    private boolean hasChildren(String bucket, String prefix) throws IOException {
        try {
            ListObjectsV2Response resp =
                    client.listObjectsV2(
                            ListObjectsV2Request.builder()
                                    .bucket(bucket)
                                    .prefix(prefix)
                                    .maxKeys(1)
                                    .build());
            return resp.keyCount() != null && resp.keyCount() > 0;
        } catch (S3Exception e) {
            throw new IOException("Failed to list s3://" + bucket + "/" + prefix, e);
        }
    }

    private void deleteKeys(String bucket, List<String> keys) throws IOException {
        // Use single-object deletes rather than the batch DeleteObjects API: the batch API requires
        // a Content-MD5 (or a checksum that several S3-compatible stores reject), whereas single
        // DeleteObject works uniformly across AWS S3, MinIO, Ceph, OSS, OBS and COS.
        try {
            for (String key : keys) {
                client.deleteObject(b -> b.bucket(bucket).key(key));
            }
        } catch (S3Exception e) {
            throw new IOException("Failed to delete objects in bucket " + bucket, e);
        }
    }

    private String bucketOf(FsPath f) {
        String authority = f.toUri().getAuthority();
        return authority != null ? authority : fsUri.getAuthority();
    }

    private static String keyOf(FsPath f) {
        String path = f.toUri().getPath();
        if (path == null || path.isEmpty()) {
            return "";
        }
        return path.startsWith("/") ? path.substring(1) : path;
    }

    private FsPath pathOfKey(String bucket, String key) {
        return new FsPath(scheme, bucket, "/" + key);
    }

    private static String stripTrailingSlash(String s) {
        return s.endsWith(DIR_MARKER_SUFFIX) ? s.substring(0, s.length() - 1) : s;
    }

    /**
     * Builds the additional-info map carried in the security token (endpoint/region/path-style).
     */
    static Map<String, String> tokenAdditionInfos(Map<String, String> s3Settings) {
        Map<String, String> infos = new LinkedHashMap<>();
        for (String k : new String[] {"endpoint", "region", "path.style.access"}) {
            String v = s3Settings.get(k);
            if (v != null) {
                infos.put(k, v);
            }
        }
        return infos;
    }
}
