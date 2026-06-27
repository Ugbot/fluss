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

import org.apache.fluss.fs.FSDataInputStream;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;

/**
 * An {@link FSDataInputStream} over a GCS object. Reads are served by a lazily-opened {@link
 * ReadChannel}; {@link #seek(long)} records the new position and seeks the underlying channel (the
 * GCS {@code ReadChannel} supports random access), matching the seek-then-read access pattern of
 * the remote-log index reads.
 */
class GsDataInputStream extends FSDataInputStream {

    private final Storage storage;
    private final String bucket;
    private final String key;

    /** Current logical position in the object. */
    private long position;

    /** The currently open channel, or {@code null} if a (re)open is needed before reading. */
    private ReadChannel channel;

    /** {@link InputStream} view of {@link #channel}, or {@code null} when the channel is closed. */
    private InputStream current;

    /** Position at which {@link #channel} is currently positioned. */
    private long currentOpenedAt = -1L;

    private boolean closed;

    GsDataInputStream(Storage storage, String bucket, String key) {
        this.storage = storage;
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
        if (channel == null) {
            try {
                channel = storage.reader(BlobId.of(bucket, key));
            } catch (Exception e) {
                throw new IOException("Failed to open gs://" + bucket + "/" + key, e);
            }
            current = Channels.newInputStream(channel);
        }
        if (currentOpenedAt != position) {
            channel.seek(position);
            currentOpenedAt = position;
        }
    }

    @Override
    public void seek(long desired) throws IOException {
        if (desired < 0) {
            throw new IOException("Cannot seek to a negative offset: " + desired);
        }
        position = desired;
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

    @Override
    public void close() throws IOException {
        closed = true;
        if (current != null) {
            try {
                current.close();
            } catch (IOException ignored) {
                // best effort; the channel is closed below
            }
            current = null;
        }
        if (channel != null) {
            channel.close();
            channel = null;
        }
        currentOpenedAt = -1L;
    }
}
