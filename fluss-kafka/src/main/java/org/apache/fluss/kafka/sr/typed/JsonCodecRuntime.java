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

package org.apache.fluss.kafka.sr.typed;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Low-level JSON Schema wire-format helpers used by the Janino-generated {@link JsonCodec
 * implementations}. Shaded Jackson is on the classpath via {@code fluss-common}; we reference it
 * explicitly here to keep the generated code's imports stable.
 *
 * <p>Scalar-only for Phase T-MF.4 (matches {@link AvroCodecRuntime} T.1 scope). Nested records /
 * arrays / maps require the {@code InternalRow} + {@code RowSerializer} pattern and are tracked in
 * T-MF.4's nested-shape follow-up.
 *
 * @since 0.9
 */
@Internal
public final class JsonCodecRuntime {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final JsonFactory FACTORY = MAPPER.getFactory();

    private JsonCodecRuntime() {}

    // ------------------------------------------------------------------------------------------
    // DECODE
    // ------------------------------------------------------------------------------------------

    /**
     * Parse the next {@code length} bytes of {@code src} starting at its current reader index as a
     * single JSON object. Returns a {@link JsonNode} the caller traverses via {@link #readInt} etc.
     */
    public static JsonNode parseObject(ByteBuf src, int length) {
        byte[] scratch = new byte[length];
        src.readBytes(scratch);
        try {
            return MAPPER.readTree(scratch);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "JSON codec failed to parse record: " + e.getMessage(), e);
        }
    }

    /** Reads an INT-typed field. Returns 0 when the field is missing or null. */
    public static int readInt(JsonNode obj, String name) {
        JsonNode v = obj.get(name);
        return v == null || v.isNull() ? 0 : v.asInt();
    }

    /** Reads a BIGINT-typed field. */
    public static long readLong(JsonNode obj, String name) {
        JsonNode v = obj.get(name);
        return v == null || v.isNull() ? 0L : v.asLong();
    }

    /** Reads a FLOAT-typed field. */
    public static float readFloat(JsonNode obj, String name) {
        JsonNode v = obj.get(name);
        return v == null || v.isNull() ? 0f : (float) v.asDouble();
    }

    /** Reads a DOUBLE-typed field. */
    public static double readDouble(JsonNode obj, String name) {
        JsonNode v = obj.get(name);
        return v == null || v.isNull() ? 0d : v.asDouble();
    }

    /** Reads a BOOLEAN-typed field. */
    public static boolean readBoolean(JsonNode obj, String name) {
        JsonNode v = obj.get(name);
        return v != null && !v.isNull() && v.asBoolean();
    }

    /** Reads a STRING-typed field. Returns an empty string when missing. */
    public static String readString(JsonNode obj, String name) {
        JsonNode v = obj.get(name);
        return v == null || v.isNull() ? "" : v.asText();
    }

    /**
     * Reads a BYTES-typed field. JSON Schema's {@code format: byte} emits base64-encoded strings;
     * we decode them back to a byte[].
     */
    public static byte[] readBytes(JsonNode obj, String name) {
        JsonNode v = obj.get(name);
        if (v == null || v.isNull()) {
            return new byte[0];
        }
        String s = v.asText();
        try {
            return Base64.getDecoder().decode(s);
        } catch (IllegalArgumentException iae) {
            // Confluent's serializer may emit raw JSON strings for {@code format: byte} depending
            // on config; fall back to UTF-8 bytes so we don't throw on that variant.
            return s.getBytes(StandardCharsets.UTF_8);
        }
    }

    /** {@code true} iff the field is present and explicitly {@code null}, or entirely absent. */
    public static boolean isNull(JsonNode obj, String name) {
        JsonNode v = obj.get(name);
        return v == null || v.isNull();
    }

    // ------------------------------------------------------------------------------------------
    // ENCODE
    // ------------------------------------------------------------------------------------------

    /** Open a new JSON object on {@code out}. Caller must pair with {@link #closeObject}. */
    public static JsonGenerator openObject(ByteBuf dst) {
        try {
            JsonGenerator gen = FACTORY.createGenerator(new ByteBufOutputStream(dst));
            gen.writeStartObject();
            return gen;
        } catch (IOException e) {
            throw new IllegalStateException("JSON codec failed to open generator: " + e, e);
        }
    }

    public static void closeObject(JsonGenerator gen) {
        try {
            gen.writeEndObject();
            gen.close();
        } catch (IOException e) {
            throw new IllegalStateException("JSON codec failed to close generator: " + e, e);
        }
    }

    public static void writeIntField(JsonGenerator gen, String name, int v) {
        try {
            gen.writeNumberField(name, v);
        } catch (IOException e) {
            throw ioerr(e);
        }
    }

    public static void writeLongField(JsonGenerator gen, String name, long v) {
        try {
            gen.writeNumberField(name, v);
        } catch (IOException e) {
            throw ioerr(e);
        }
    }

    public static void writeFloatField(JsonGenerator gen, String name, float v) {
        try {
            gen.writeNumberField(name, v);
        } catch (IOException e) {
            throw ioerr(e);
        }
    }

    public static void writeDoubleField(JsonGenerator gen, String name, double v) {
        try {
            gen.writeNumberField(name, v);
        } catch (IOException e) {
            throw ioerr(e);
        }
    }

    public static void writeBooleanField(JsonGenerator gen, String name, boolean v) {
        try {
            gen.writeBooleanField(name, v);
        } catch (IOException e) {
            throw ioerr(e);
        }
    }

    public static void writeStringField(JsonGenerator gen, String name, String v) {
        try {
            gen.writeStringField(name, v);
        } catch (IOException e) {
            throw ioerr(e);
        }
    }

    public static void writeBytesField(JsonGenerator gen, String name, byte[] v) {
        try {
            gen.writeStringField(name, Base64.getEncoder().encodeToString(v));
        } catch (IOException e) {
            throw ioerr(e);
        }
    }

    public static void writeNullField(JsonGenerator gen, String name) {
        try {
            gen.writeNullField(name);
        } catch (IOException e) {
            throw ioerr(e);
        }
    }

    private static IllegalStateException ioerr(IOException e) {
        return new IllegalStateException("JSON codec IO error: " + e.getMessage(), e);
    }

    /** Minimal OutputStream adapter that writes into a Netty {@link ByteBuf}. */
    private static final class ByteBufOutputStream extends OutputStream {
        private final ByteBuf buf;

        ByteBufOutputStream(ByteBuf buf) {
            this.buf = buf;
        }

        @Override
        public void write(int b) {
            buf.writeByte(b);
        }

        @Override
        public void write(byte[] b, int off, int len) {
            buf.writeBytes(b, off, len);
        }
    }
}
