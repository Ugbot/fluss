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
import org.apache.fluss.exception.FlussRuntimeException;

/**
 * Thrown by a {@link FormatTranslator} when a source schema contains a shape that cannot be mapped
 * into a Fluss {@link org.apache.fluss.types.RowType}.
 *
 * <p>Typical rejections (per plan §22.5):
 *
 * <ul>
 *   <li>Unions other than the nullable-union idiom {@code [null, T]}. Fluss's row model has no sum
 *       types.
 *   <li>Recursive / self-referential named types. Fluss row types are strictly tree-shaped.
 *   <li>Avro {@code fixed(n)} where {@code n} exceeds the reasonable column-property budget.
 *   <li>Logical {@code decimal(p, s)} with precision / scale outside the Fluss {@code DECIMAL(p,s)}
 *       supported range (0 &lt; scale &le; precision, precision &le; 38).
 *   <li>Empty records, or record fields whose names collide after identifier escaping.
 * </ul>
 *
 * @since 0.9
 */
@PublicEvolving
public class SchemaTranslationException extends FlussRuntimeException {
    private static final long serialVersionUID = 1L;

    public SchemaTranslationException(String message) {
        super(message);
    }

    public SchemaTranslationException(String message, Throwable cause) {
        super(message, cause);
    }
}
