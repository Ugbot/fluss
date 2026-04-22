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

package org.apache.fluss.kafka.sr;

import org.apache.fluss.annotation.Internal;

/**
 * Resolves Confluent SR subject names onto Fluss topic names. Phase A1 supports only
 * TopicNameStrategy value subjects ({@code <topic>-value}). Key subjects ({@code <topic>-key}) and
 * the other Confluent naming strategies surface as {@link SchemaRegistryException} with HTTP 400.
 */
@Internal
public final class SubjectResolver {

    private SubjectResolver() {}

    public static String topicFromValueSubject(String subject) {
        if (subject == null || subject.isEmpty()) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.INVALID_INPUT, "subject is required");
        }
        if (subject.endsWith("-key")) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.UNSUPPORTED,
                    "key subjects are not yet supported (Phase A1)");
        }
        if (!subject.endsWith("-value")) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.UNSUPPORTED,
                    "only TopicNameStrategy value subjects (<topic>-value) are supported in Phase A1");
        }
        String topic = subject.substring(0, subject.length() - "-value".length());
        if (topic.isEmpty()) {
            throw new SchemaRegistryException(
                    SchemaRegistryException.Kind.INVALID_INPUT, "subject topic part is empty");
        }
        return topic;
    }

    public static String valueSubjectForTopic(String topic) {
        return topic + "-value";
    }
}
