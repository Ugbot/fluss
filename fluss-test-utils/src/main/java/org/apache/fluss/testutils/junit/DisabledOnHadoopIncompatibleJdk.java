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

package org.apache.fluss.testutils.junit;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Disables a test (class or method) on JDKs where Apache Hadoop cannot run.
 *
 * <p>Hadoop's {@code UserGroupInformation} calls {@code javax.security.auth.Subject.getSubject},
 * which throws {@code UnsupportedOperationException} on JDK 24+ where the Security Manager is
 * permanently disabled (JEP 486). No released Hadoop carries the {@code Subject.current()} fix
 * (HADOOP-19212) yet, so any test that exercises a Hadoop code path (HadoopCatalog, HDFS, Hudi)
 * cannot run on those JDKs. Such tests are part of the opt-in Hadoop path; the default, Hadoop-free
 * path is unaffected.
 *
 * <p>Apply to test classes (or base classes) that build a Hadoop catalog / file system. Once a
 * fixed Hadoop is adopted this annotation can be removed.
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(DisabledOnHadoopIncompatibleJdk.Condition.class)
public @interface DisabledOnHadoopIncompatibleJdk {

    /** The first JDK feature version on which Hadoop's UserGroupInformation is broken. */
    int BROKEN_FROM_FEATURE_VERSION = 24;

    /** {@link ExecutionCondition} backing {@link DisabledOnHadoopIncompatibleJdk}. */
    class Condition implements ExecutionCondition {

        @Override
        public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
            int feature = Runtime.version().feature();
            if (feature >= BROKEN_FROM_FEATURE_VERSION) {
                return ConditionEvaluationResult.disabled(
                        "Hadoop UserGroupInformation uses Subject.getSubject, which is unsupported "
                                + "on JDK "
                                + feature
                                + " (JEP 486). This Hadoop-path test runs on JDK < "
                                + BROKEN_FROM_FEATURE_VERSION
                                + " or once a fixed Hadoop is adopted.");
            }
            return ConditionEvaluationResult.enabled("JDK " + feature + " supports Hadoop UGI");
        }
    }
}
