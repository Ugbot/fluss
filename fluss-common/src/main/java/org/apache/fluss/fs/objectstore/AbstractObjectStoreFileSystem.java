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

package org.apache.fluss.fs.objectstore;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A base {@link FileSystem} for flat object stores (S3, GCS, Azure Blob, ...) that emulates the
 * hierarchical directory semantics required by the Fluss {@link FileSystem} contract (see {@code
 * FileSystemBehaviorTestSuite}) over a flat key namespace.
 *
 * <p>Directories are emulated with zero-byte marker objects whose key ends in {@code "/"}:
 *
 * <ul>
 *   <li>{@code mkdirs(p)} writes a marker at {@code <key>/} after verifying that no object (file)
 *       exists at {@code p} or at any ancestor path.
 *   <li>{@code getFileStatus(p)} reports a file when a HEAD of {@code <key>} succeeds, a directory
 *       when any object exists under {@code <key>/}, otherwise throws {@link
 *       FileNotFoundException}.
 *   <li>{@code listStatus(p)} lists the immediate children of {@code <key>/} (objects minus the
 *       self marker become files; common prefixes become sub-directories).
 *   <li>recursive {@code delete} removes every object under {@code <key>/}; non-recursive delete of
 *       a non-empty directory fails.
 * </ul>
 *
 * <p>Subclasses implement a handful of primitive operations against their store SDK; all of the
 * directory bookkeeping lives here so every native object-store plugin shares one tested
 * implementation. The {@link #open}, {@link #create} and {@code obtainSecurityToken} stream/token
 * operations remain store-specific and are declared abstract by {@link FileSystem}.
 */
@Internal
public abstract class AbstractObjectStoreFileSystem extends FileSystem {

    /** Suffix used to mark a key as an (emulated) directory. */
    protected static final String DIR_MARKER_SUFFIX = "/";

    private final String scheme;
    private final URI fsUri;

    protected AbstractObjectStoreFileSystem(String scheme, URI fsUri) {
        this.scheme = scheme;
        this.fsUri = fsUri;
    }

    // ------------------------------------------------------------------------
    //  primitive operations implemented per store
    // ------------------------------------------------------------------------

    /** Returns the object length if a file exists at the key, otherwise an empty optional. */
    protected abstract OptionalLong headLength(String bucket, String key) throws IOException;

    /** Returns whether at least one object exists under the given prefix. */
    protected abstract boolean hasChildrenUnder(String bucket, String prefix) throws IOException;

    /** Lists the immediate children of a directory prefix (delimited by {@code "/"}). */
    protected abstract List<ObjectEntry> listImmediateChildren(String bucket, String dirPrefix)
            throws IOException;

    /** Lists every object key under the given prefix (no delimiter), used for recursive delete. */
    protected abstract List<String> listAllKeys(String bucket, String prefix) throws IOException;

    /** Writes a zero-byte object at the given key (used to materialize a directory marker). */
    protected abstract void putEmptyObject(String bucket, String key) throws IOException;

    /** Deletes a single object; deleting a non-existent key must not fail. */
    protected abstract void deleteObject(String bucket, String key) throws IOException;

    /** Copies a single object within the same store. */
    protected abstract void copyObject(
            String srcBucket, String srcKey, String dstBucket, String dstKey) throws IOException;

    // ------------------------------------------------------------------------
    //  shared FileSystem implementation
    // ------------------------------------------------------------------------

    @Override
    public URI getUri() {
        return fsUri;
    }

    @Override
    public FileStatus getFileStatus(FsPath f) throws IOException {
        String bucket = bucketOf(f);
        String key = keyOf(f);
        if (key.isEmpty()) {
            // bucket root is always a directory
            return new ObjectStoreFileStatus(f, 0L, true);
        }
        OptionalLong len = headLength(bucket, key);
        if (len.isPresent()) {
            return new ObjectStoreFileStatus(f, len.getAsLong(), false);
        }
        if (hasChildrenUnder(bucket, key + DIR_MARKER_SUFFIX)) {
            return new ObjectStoreFileStatus(f, 0L, true);
        }
        throw new FileNotFoundException("File " + f + " does not exist");
    }

    @Override
    public FileStatus[] listStatus(FsPath f) throws IOException {
        String bucket = bucketOf(f);
        String key = keyOf(f);

        // a file lists as itself, mirroring LocalFileSystem
        if (!key.isEmpty()) {
            OptionalLong len = headLength(bucket, key);
            if (len.isPresent()) {
                return new FileStatus[] {new ObjectStoreFileStatus(f, len.getAsLong(), false)};
            }
        }

        String prefix = key.isEmpty() ? "" : key + DIR_MARKER_SUFFIX;
        List<ObjectEntry> children = listImmediateChildren(bucket, prefix);
        List<FileStatus> results = new ArrayList<>(children.size());
        for (ObjectEntry e : children) {
            if (!e.isDir && e.key.equals(prefix)) {
                // the directory's own marker, not a child
                continue;
            }
            results.add(new ObjectStoreFileStatus(pathOfKey(bucket, e.key), e.size, e.isDir));
        }
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
        if (headLength(bucket, key).isPresent()) {
            deleteObject(bucket, key);
            return true;
        }

        // case 2: an (emulated) directory
        String dirPrefix = key + DIR_MARKER_SUFFIX;
        List<String> allKeys = listAllKeys(bucket, dirPrefix);
        if (allKeys.isEmpty()) {
            // nothing exists at this path; deletion is vacuously successful
            return true;
        }
        boolean hasNonMarkerChild = false;
        for (String k : allKeys) {
            if (!k.equals(dirPrefix)) {
                hasNonMarkerChild = true;
                break;
            }
        }
        if (!recursive && hasNonMarkerChild) {
            throw new IOException("Directory " + f + " is not empty");
        }
        for (String k : allKeys) {
            deleteObject(bucket, k);
        }
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
        if (headLength(bucket, key).isPresent()) {
            throw new IOException("Cannot create directory " + f + ": a file exists at that path");
        }
        // no ancestor may be a file
        FsPath parent = f.getParent();
        while (parent != null) {
            String parentKey = keyOf(parent);
            if (parentKey.isEmpty()) {
                break;
            }
            if (headLength(bucket, parentKey).isPresent()) {
                throw new IOException(
                        "Cannot create directory " + f + ": a file exists at ancestor " + parent);
            }
            parent = parent.getParent();
        }

        putEmptyObject(bucket, key + DIR_MARKER_SUFFIX);
        return true;
    }

    @Override
    public boolean rename(FsPath src, FsPath dst) throws IOException {
        // Object stores have no atomic rename. Not used on any Fluss core hot path; implemented as
        // copy-then-delete over the (single) source object for correctness.
        String bucket = bucketOf(src);
        String srcKey = keyOf(src);
        if (!headLength(bucket, srcKey).isPresent()) {
            return false;
        }
        copyObject(bucket, srcKey, bucketOf(dst), keyOf(dst));
        deleteObject(bucket, srcKey);
        return true;
    }

    // ------------------------------------------------------------------------
    //  shared helpers
    // ------------------------------------------------------------------------

    /** Returns the bucket (URI authority) of a path, falling back to this file system's bucket. */
    protected String bucketOf(FsPath f) {
        String authority = f.toUri().getAuthority();
        return authority != null ? authority : fsUri.getAuthority();
    }

    /** Returns the object key (path without the leading slash) of a path. */
    protected static String keyOf(FsPath f) {
        String path = f.toUri().getPath();
        if (path == null || path.isEmpty()) {
            return "";
        }
        return path.startsWith("/") ? path.substring(1) : path;
    }

    /** Builds the {@link FsPath} for an object key in the given bucket. */
    protected FsPath pathOfKey(String bucket, String key) {
        return new FsPath(scheme, bucket, "/" + stripTrailingSlash(key));
    }

    protected static String stripTrailingSlash(String s) {
        return s.endsWith(DIR_MARKER_SUFFIX) ? s.substring(0, s.length() - 1) : s;
    }

    /** An immediate child of a directory: either a file (with size) or a sub-directory. */
    protected static final class ObjectEntry {
        final String key;
        final long size;
        final boolean isDir;

        public ObjectEntry(String key, long size, boolean isDir) {
            this.key = key;
            this.size = size;
            this.isDir = isDir;
        }

        /** A file child with the given full key and size. */
        public static ObjectEntry file(String key, long size) {
            return new ObjectEntry(key, size, false);
        }

        /** A sub-directory child for the given prefix (with or without trailing slash). */
        public static ObjectEntry dir(String prefix) {
            return new ObjectEntry(stripTrailingSlash(prefix), 0L, true);
        }
    }

    /** A {@link FileStatus} for an object (file) or an emulated directory. */
    private static final class ObjectStoreFileStatus implements FileStatus {
        private final FsPath path;
        private final long len;
        private final boolean isDir;

        ObjectStoreFileStatus(FsPath path, long len, boolean isDir) {
            this.path = path;
            this.len = len;
            this.isDir = isDir;
        }

        @Override
        public long getLen() {
            return len;
        }

        @Override
        public boolean isDir() {
            return isDir;
        }

        @Override
        public FsPath getPath() {
            return path;
        }
    }
}
