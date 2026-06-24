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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.types.RowType;

/**
 * Bidirectional translator between a source schema language (Avro / JSON Schema / Protobuf) and a
 * Fluss {@link RowType}.
 *
 * <p>Plan §22.2 / §22.5 foundation: we keep Fluss-first authority on the column catalogue, so the
 * translator answers two questions at register / evolve time:
 *
 * <ul>
 *   <li>{@link #translateTo(String)}: given an incoming schema text, what {@link RowType} should
 *       back the Fluss table?
 *   <li>{@link #translateFrom(RowType)}: given a Fluss table's {@link RowType}, what schema text
 *       should we return to a client that asks for it?
 * </ul>
 *
 * <p>The round trip is <em>not</em> lossless — whitespace, comments, field aliases, default values,
 * and any non-mappable annotations are dropped on the way in. Callers that need exact wire bytes
 * must cache the original schema text separately.
 *
 * <p>Implementations MUST be thread-safe: a single instance is shared across all produce / fetch
 * threads on a broker. They SHOULD be stateless.
 *
 * @since 0.9
 */
@PublicEvolving
public interface FormatTranslator {

    /**
     * Parse the source schema and map it into a Fluss {@link RowType}.
     *
     * @param schemaText the source schema text (format-specific: Avro JSON, etc.)
     * @return the mapped row type
     * @throws SchemaTranslationException when the schema contains a shape that cannot be mapped
     *     (e.g. non-nullable unions, recursive references)
     */
    RowType translateTo(String schemaText);

    /**
     * Render the given {@link RowType} back as a source schema text.
     *
     * <p>Accepts information loss: whitespace, comments, and non-structural annotations are not
     * preserved.
     *
     * @param rowType the Fluss row type
     * @return a schema text in this translator's format
     * @throws SchemaTranslationException if the row type cannot be expressed in this format
     */
    String translateFrom(RowType rowType);

    /**
     * A short stable identifier for this translator's format (e.g. {@code "AVRO"}). Used by the
     * bolt-on wiring when caching codecs and when returning a {@code schemaType} from Schema
     * Registry endpoints.
     *
     * @return the format id
     */
    String formatId();
}
