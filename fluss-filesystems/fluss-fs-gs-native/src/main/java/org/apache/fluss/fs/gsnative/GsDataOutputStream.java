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

import org.apache.fluss.fs.FSDataOutputStream;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;

/**
 * An {@link FSDataOutputStream} that buffers all written bytes to a local temp file and uploads the
 * object on {@link #close()}. This honors the FileSystem persistence contract (data is durable only
 * after {@code close()} returns) and matches GCS's commit-on-finalize semantics. The {@link
 * Storage#createFrom} path performs a resumable (chunked) upload for large files automatically.
 */
class GsDataOutputStream extends FSDataOutputStream {

    private final Storage storage;
    private final String bucket;
    private final String key;
    private final File tempFile;
    private final OutputStream tempOut;

    private long bytesWritten;
    private boolean closed;

    GsDataOutputStream(Storage storage, String bucket, String key) throws IOException {
        this.storage = storage;
        this.bucket = bucket;
        this.key = key;
        this.tempFile = Files.createTempFile("fluss-gs-native-", ".tmp").toFile();
        this.tempOut = new BufferedOutputStream(Files.newOutputStream(tempFile.toPath()));
    }

    @Override
    public long getPos() {
        return bytesWritten;
    }

    @Override
    public void write(int b) throws IOException {
        tempOut.write(b);
        bytesWritten++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        tempOut.write(b, off, len);
        bytesWritten += len;
    }

    @Override
    public void flush() throws IOException {
        // The bytes are only durable in GCS after close(); flushing the local buffer is the most we
        // can guarantee mid-stream.
        tempOut.flush();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            tempOut.close();
            storage.createFrom(
                    BlobInfo.newBuilder(BlobId.of(bucket, key)).build(), tempFile.toPath());
        } catch (Exception e) {
            throw new IOException("Failed to upload gs://" + bucket + "/" + key, e);
        } finally {
            // best effort cleanup of the local spill file
            //noinspection ResultOfMethodCallIgnored
            tempFile.delete();
        }
    }
}
