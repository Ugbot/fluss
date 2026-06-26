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

package org.apache.fluss.lake.iceberg.utils;

import org.apache.fluss.annotation.VisibleForTesting;

import org.apache.iceberg.CatalogProperties;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.Locale;
import java.util.Map;

/**
 * Selects the Iceberg {@link org.apache.iceberg.io.FileIO} implementation used by the lake tiering
 * write/read path.
 *
 * <p>Iceberg defaults to {@code HadoopFileIO}, which requires {@code hadoop-common} (and {@code
 * hadoop-aws} for S3) on the classpath. To support the dependency-diet goal of running the S3
 * tiering write path without Hadoop, this configurer switches Iceberg to {@code
 * org.apache.iceberg.aws.s3.S3FileIO} (AWS SDK v2, no Hadoop) whenever the warehouse location uses
 * an S3 scheme ({@code s3://} or {@code s3a://}).
 *
 * <p>For non-S3 locations (HDFS, local files) the FileIO is left untouched so {@code HadoopFileIO}
 * remains the fallback and nothing regresses.
 *
 * <p>Selection rules (highest precedence first):
 *
 * <ol>
 *   <li>If the user already set {@code io-impl} ({@link CatalogProperties#FILE_IO_IMPL})
 *       explicitly, it is always honored and never overridden.
 *   <li>If {@code fluss.s3.file-io.enabled} is set, its boolean value forces S3FileIO on/off
 *       regardless of the warehouse scheme.
 *   <li>Otherwise S3FileIO is selected automatically when the {@code warehouse} location (or, when
 *       absent, the catalog {@code uri}) uses an {@code s3}/{@code s3a} scheme.
 * </ol>
 */
public final class IcebergFileIOConfigurer {

    /** Fully-qualified class name of Iceberg's AWS SDK v2 based S3 FileIO. */
    public static final String S3_FILE_IO_IMPL = "org.apache.iceberg.aws.s3.S3FileIO";

    /**
     * Catalog property to force-enable ({@code true}) or force-disable ({@code false}) selection of
     * {@code S3FileIO}. When absent, selection is auto-detected from the warehouse scheme. Lives in
     * the {@code fluss.} namespace so it never collides with Iceberg's own {@code s3.*} properties.
     */
    public static final String S3_FILE_IO_ENABLED_KEY = "fluss.s3.file-io.enabled";

    private IcebergFileIOConfigurer() {}

    /**
     * Applies FileIO selection in-place to the given Iceberg catalog properties map.
     *
     * @param icebergProps the mutable Iceberg catalog property map (e.g. {@code warehouse}, {@code
     *     type}, {@code catalog-impl}); modified in place.
     */
    public static void configureFileIO(Map<String, String> icebergProps) {
        // Rule 1: an explicit io-impl always wins.
        if (icebergProps.containsKey(CatalogProperties.FILE_IO_IMPL)) {
            return;
        }

        if (shouldUseS3FileIO(icebergProps)) {
            icebergProps.put(CatalogProperties.FILE_IO_IMPL, S3_FILE_IO_IMPL);
        }
    }

    @VisibleForTesting
    static boolean shouldUseS3FileIO(Map<String, String> icebergProps) {
        // Rule 2: explicit override via fluss.s3.file-io.enabled.
        String enabled = icebergProps.get(S3_FILE_IO_ENABLED_KEY);
        if (enabled != null) {
            return Boolean.parseBoolean(enabled.trim());
        }

        // Rule 3: auto-detect from the storage location scheme.
        String location = icebergProps.get(CatalogProperties.WAREHOUSE_LOCATION);
        if (location == null) {
            location = icebergProps.get(CatalogProperties.URI);
        }
        return isS3Location(location);
    }

    @VisibleForTesting
    static boolean isS3Location(@Nullable String location) {
        if (location == null) {
            return false;
        }
        String scheme = schemeOf(location);
        if (scheme == null) {
            return false;
        }
        scheme = scheme.toLowerCase(Locale.ROOT);
        return scheme.equals("s3") || scheme.equals("s3a") || scheme.equals("s3n");
    }

    @Nullable
    private static String schemeOf(String location) {
        try {
            String scheme = URI.create(location).getScheme();
            if (scheme != null) {
                return scheme;
            }
        } catch (IllegalArgumentException ignored) {
            // Fall through to manual parsing for locations URI.create rejects.
        }
        int idx = location.indexOf("://");
        if (idx > 0) {
            return location.substring(0, idx);
        }
        return null;
    }
}
