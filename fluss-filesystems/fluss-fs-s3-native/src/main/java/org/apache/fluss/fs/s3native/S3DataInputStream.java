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

import org.apache.fluss.fs.FSDataInputStream;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.IOException;
import java.io.InputStream;

/**
 * An {@link FSDataInputStream} over an S3 object. Reads are served by a lazily-opened ranged {@code
 * GetObject}; {@link #seek(long)} simply records the new position and the next read re-opens the
 * object from that offset, which matches the seek-then-read access pattern of the remote-log index
 * reads.
 */
class S3DataInputStream extends FSDataInputStream {

    private final S3Client client;
    private final String bucket;
    private final String key;

    /** Current logical position in the object. */
    private long position;

    /** The currently open ranged stream, or {@code null} if a (re)open is needed before reading. */
    private InputStream current;

    /** Position at which {@link #current} was opened, used to detect when a reopen is required. */
    private long currentOpenedAt = -1L;

    private boolean closed;

    S3DataInputStream(S3Client client, String bucket, String key) {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream already closed");
        }
        if (current != null && currentOpenedAt == position) {
            return;
        }
        closeCurrent();
        GetObjectRequest.Builder req = GetObjectRequest.builder().bucket(bucket).key(key);
        if (position > 0) {
            // HTTP range is inclusive; request from the current position to the end of the object.
            req.range("bytes=" + position + "-");
        }
        try {
            current = client.getObject(req.build());
        } catch (Exception e) {
            throw new IOException(
                    "Failed to open s3://" + bucket + "/" + key + " at " + position, e);
        }
        currentOpenedAt = position;
    }

    @Override
    public void seek(long desired) throws IOException {
        if (desired < 0) {
            throw new IOException("Cannot seek to a negative offset: " + desired);
        }
        if (desired != position) {
            position = desired;
            // Defer the actual reopen until the next read so a seek without a following read is
            // free.
            closeCurrent();
        }
    }

    @Override
    public long getPos() {
        return position;
    }

    @Override
    public int read() throws IOException {
        ensureOpen();
        int b = current.read();
        if (b >= 0) {
            position++;
            currentOpenedAt = position;
        }
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        ensureOpen();
        int n = current.read(b, off, len);
        if (n > 0) {
            position += n;
            currentOpenedAt = position;
        }
        return n;
    }

    @Override
    public int available() throws IOException {
        ensureOpen();
        return current.available();
    }

    private void closeCurrent() {
        if (current != null) {
            try {
                current.close();
            } catch (IOException ignored) {
                // best effort; a new stream will be opened on next read
            }
            current = null;
            currentOpenedAt = -1L;
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
        closeCurrent();
    }
}
