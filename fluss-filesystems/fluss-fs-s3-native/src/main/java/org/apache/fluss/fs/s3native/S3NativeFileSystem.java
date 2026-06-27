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
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.objectstore.AbstractObjectStoreFileSystem;
import org.apache.fluss.fs.token.Credentials;
import org.apache.fluss.fs.token.CredentialsJsonSerde;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/**
 * A {@link org.apache.fluss.fs.FileSystem} for S3 (and S3-compatible stores) built directly on the
 * AWS SDK v2, with no Hadoop dependency. The hierarchical directory emulation lives in {@link
 * AbstractObjectStoreFileSystem}; this class only implements the S3 primitive operations.
 */
@Internal
public class S3NativeFileSystem extends AbstractObjectStoreFileSystem {

    private final S3Client client;

    /** Static credentials, if configured, used to vend a security token to clients. */
    @Nullable private final Credentials staticCredentials;

    private final Map<String, String> tokenAdditionInfos;
    private final String scheme;

    S3NativeFileSystem(
            String scheme,
            URI fsUri,
            S3Client client,
            @Nullable Credentials staticCredentials,
            Map<String, String> tokenAdditionInfos) {
        super(scheme, fsUri);
        this.scheme = scheme;
        this.client = client;
        this.staticCredentials = staticCredentials;
        this.tokenAdditionInfos = tokenAdditionInfos;
    }

    @Override
    public ObtainedSecurityToken obtainSecurityToken() throws IOException {
        // v1: pass static credentials through to clients when configured; otherwise no token (each
        // node resolves credentials from its own default provider chain / instance profile).
        byte[] token =
                staticCredentials == null
                        ? new byte[0]
                        : CredentialsJsonSerde.toJson(staticCredentials);
        return new ObtainedSecurityToken(scheme, token, null, tokenAdditionInfos);
    }

    @Override
    public FSDataInputStream open(FsPath f) throws IOException {
        return new S3DataInputStream(client, bucketOf(f), keyOf(f));
    }

    @Override
    public FSDataOutputStream create(FsPath f, WriteMode overwriteMode) throws IOException {
        String bucket = bucketOf(f);
        String key = keyOf(f);
        if (overwriteMode == WriteMode.NO_OVERWRITE && headLength(bucket, key).isPresent()) {
            throw new IOException("File already exists: " + f);
        }
        return new S3DataOutputStream(client, bucket, key);
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
            return OptionalLong.of(
                    client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build())
                            .contentLength());
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return OptionalLong.empty();
            }
            throw new IOException("Failed to HEAD s3://" + bucket + "/" + key, e);
        }
    }

    @Override
    protected boolean hasChildrenUnder(String bucket, String prefix) throws IOException {
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

    @Override
    protected List<ObjectEntry> listImmediateChildren(String bucket, String dirPrefix)
            throws IOException {
        List<ObjectEntry> results = new ArrayList<>();
        String continuationToken = null;
        try {
            do {
                ListObjectsV2Request.Builder req =
                        ListObjectsV2Request.builder()
                                .bucket(bucket)
                                .prefix(dirPrefix)
                                .delimiter(DIR_MARKER_SUFFIX);
                if (continuationToken != null) {
                    req.continuationToken(continuationToken);
                }
                ListObjectsV2Response resp = client.listObjectsV2(req.build());
                for (CommonPrefix cp : resp.commonPrefixes()) {
                    results.add(ObjectEntry.dir(cp.prefix()));
                }
                for (S3Object o : resp.contents()) {
                    results.add(ObjectEntry.file(o.key(), o.size()));
                }
                continuationToken =
                        Boolean.TRUE.equals(resp.isTruncated())
                                ? resp.nextContinuationToken()
                                : null;
            } while (continuationToken != null);
        } catch (S3Exception e) {
            throw new IOException("Failed to list s3://" + bucket + "/" + dirPrefix, e);
        }
        return results;
    }

    @Override
    protected List<String> listAllKeys(String bucket, String prefix) throws IOException {
        List<String> keys = new ArrayList<>();
        String continuationToken = null;
        try {
            do {
                ListObjectsV2Request.Builder req =
                        ListObjectsV2Request.builder().bucket(bucket).prefix(prefix);
                if (continuationToken != null) {
                    req.continuationToken(continuationToken);
                }
                ListObjectsV2Response resp = client.listObjectsV2(req.build());
                for (S3Object o : resp.contents()) {
                    keys.add(o.key());
                }
                continuationToken =
                        Boolean.TRUE.equals(resp.isTruncated())
                                ? resp.nextContinuationToken()
                                : null;
            } while (continuationToken != null);
        } catch (S3Exception e) {
            throw new IOException("Failed to list s3://" + bucket + "/" + prefix, e);
        }
        return keys;
    }

    @Override
    protected void putEmptyObject(String bucket, String key) throws IOException {
        try {
            client.putObject(
                    PutObjectRequest.builder().bucket(bucket).key(key).build(),
                    RequestBody.empty());
        } catch (S3Exception e) {
            throw new IOException("Failed to write s3://" + bucket + "/" + key, e);
        }
    }

    @Override
    protected void deleteObject(String bucket, String key) throws IOException {
        // Single-object DeleteObject (not the batch API): the batch DeleteObjects requires a
        // Content-MD5 / checksum that several S3-compatible stores (MinIO, Ceph, OSS) reject.
        try {
            client.deleteObject(b -> b.bucket(bucket).key(key));
        } catch (S3Exception e) {
            throw new IOException("Failed to delete s3://" + bucket + "/" + key, e);
        }
    }

    @Override
    protected void copyObject(String srcBucket, String srcKey, String dstBucket, String dstKey)
            throws IOException {
        try {
            client.copyObject(
                    b ->
                            b.sourceBucket(srcBucket)
                                    .sourceKey(srcKey)
                                    .destinationBucket(dstBucket)
                                    .destinationKey(dstKey));
        } catch (S3Exception e) {
            throw new IOException(
                    "Failed to copy s3://"
                            + srcBucket
                            + "/"
                            + srcKey
                            + " to s3://"
                            + dstBucket
                            + "/"
                            + dstKey,
                    e);
        }
    }

    /**
     * Builds the additional-info map carried in the security token (endpoint/region/path-style).
     */
    static Map<String, String> tokenAdditionInfos(Map<String, String> s3Settings) {
        Map<String, String> infos = new java.util.LinkedHashMap<>();
        for (String k : new String[] {"endpoint", "region", "path.style.access"}) {
            String v = s3Settings.get(k);
            if (v != null) {
                infos.put(k, v);
            }
        }
        return infos;
    }
}
