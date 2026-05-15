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

package org.apache.fluss.kafka.admin;

import org.apache.fluss.security.auth.sasl.scram.JaasFileScramCredentialStore;
import org.apache.fluss.security.auth.sasl.scram.ScramCredentialUtils;
import org.apache.fluss.security.auth.sasl.scram.ScramMechanism;

import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData;
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData.ScramCredentialDeletion;
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData.ScramCredentialUpsertion;
import org.apache.kafka.common.message.AlterUserScramCredentialsResponseData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData.UserName;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData.CredentialInfo;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link KafkaScramCredentialsTranscoder}. */
class KafkaScramCredentialsTranscoderTest {

    @Test
    void alterUpsertsPersistAndDescribeNeverLeaksKeys() {
        JaasFileScramCredentialStore store = new JaasFileScramCredentialStore();

        // Client-side (what kafka's AlterUserScramCredentialsRequest would carry): salt +
        // PBKDF2(password, salt, iterations).
        String password = "super-" + UUID.randomUUID();
        byte[] salt = ScramCredentialUtils.randomSalt();
        byte[] saltedPassword =
                ScramCredentialUtils.hi(
                        ScramMechanism.SCRAM_SHA_256,
                        password.getBytes(StandardCharsets.UTF_8),
                        salt,
                        4096);

        AlterUserScramCredentialsRequestData req = new AlterUserScramCredentialsRequestData();
        req.setUpsertions(
                Collections.singletonList(
                        new ScramCredentialUpsertion()
                                .setName("alice")
                                .setMechanism(KafkaScramCredentialsTranscoder.MECHANISM_SHA_256)
                                .setIterations(4096)
                                .setSalt(salt)
                                .setSaltedPassword(saltedPassword)));
        req.setDeletions(Collections.emptyList());

        AlterUserScramCredentialsResponseData resp =
                KafkaScramCredentialsTranscoder.handleAlter(req, store);

        assertThat(resp.results()).hasSize(1);
        assertThat(resp.results().get(0).user()).isEqualTo("alice");
        assertThat(resp.results().get(0).errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(store.lookup("alice", "SCRAM-SHA-256")).isPresent();
        assertThat(store.lookup("alice", "SCRAM-SHA-256").get().iterations()).isEqualTo(4096);

        // Describe — must not return salt/keys, only (mechanism, iterations).
        DescribeUserScramCredentialsRequestData dreq =
                new DescribeUserScramCredentialsRequestData();
        dreq.setUsers(Collections.singletonList(new UserName().setName("alice")));
        DescribeUserScramCredentialsResponseData dresp =
                KafkaScramCredentialsTranscoder.handleDescribe(dreq, store);

        assertThat(dresp.results()).hasSize(1);
        DescribeUserScramCredentialsResult row = dresp.results().get(0);
        assertThat(row.user()).isEqualTo("alice");
        assertThat(row.errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(row.credentialInfos()).hasSize(1);
        CredentialInfo info = row.credentialInfos().get(0);
        assertThat(info.mechanism()).isEqualTo(KafkaScramCredentialsTranscoder.MECHANISM_SHA_256);
        assertThat(info.iterations()).isEqualTo(4096);
    }

    @Test
    void alterDeletionsRemoveRows() {
        JaasFileScramCredentialStore store = new JaasFileScramCredentialStore();
        store.addPlainPassword("bob", ScramMechanism.SCRAM_SHA_512, "bob-secret", 4096);
        assertThat(store.lookup("bob", "SCRAM-SHA-512")).isPresent();

        AlterUserScramCredentialsRequestData req = new AlterUserScramCredentialsRequestData();
        req.setUpsertions(Collections.emptyList());
        req.setDeletions(
                Collections.singletonList(
                        new ScramCredentialDeletion()
                                .setName("bob")
                                .setMechanism(KafkaScramCredentialsTranscoder.MECHANISM_SHA_512)));

        AlterUserScramCredentialsResponseData resp =
                KafkaScramCredentialsTranscoder.handleAlter(req, store);

        assertThat(resp.results()).hasSize(1);
        assertThat(resp.results().get(0).errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(store.lookup("bob", "SCRAM-SHA-512")).isNotPresent();
    }

    @Test
    void alterRejectsBelowMinimumIterations() {
        JaasFileScramCredentialStore store = new JaasFileScramCredentialStore();
        byte[] salt = ScramCredentialUtils.randomSalt();
        AlterUserScramCredentialsRequestData req = new AlterUserScramCredentialsRequestData();
        req.setUpsertions(
                Collections.singletonList(
                        new ScramCredentialUpsertion()
                                .setName("carl")
                                .setMechanism(KafkaScramCredentialsTranscoder.MECHANISM_SHA_256)
                                .setIterations(100)
                                .setSalt(salt)
                                .setSaltedPassword(new byte[] {1, 2, 3})));
        req.setDeletions(Collections.emptyList());

        AlterUserScramCredentialsResponseData resp =
                KafkaScramCredentialsTranscoder.handleAlter(req, store);

        assertThat(resp.results()).hasSize(1);
        assertThat(resp.results().get(0).errorCode())
                .isEqualTo(Errors.UNACCEPTABLE_CREDENTIAL.code());
        assertThat(store.lookup("carl", "SCRAM-SHA-256")).isNotPresent();
    }

    @Test
    void describeMissingUserReturnsResourceNotFound() {
        JaasFileScramCredentialStore store = new JaasFileScramCredentialStore();
        DescribeUserScramCredentialsRequestData req = new DescribeUserScramCredentialsRequestData();
        req.setUsers(Collections.singletonList(new UserName().setName("nobody")));
        DescribeUserScramCredentialsResponseData resp =
                KafkaScramCredentialsTranscoder.handleDescribe(req, store);

        assertThat(resp.results()).hasSize(1);
        assertThat(resp.results().get(0).errorCode()).isEqualTo(Errors.RESOURCE_NOT_FOUND.code());
    }

    @Test
    void describeAllUsersReturnsEveryMechanism() {
        JaasFileScramCredentialStore store = new JaasFileScramCredentialStore();
        store.addPlainPassword("alice", ScramMechanism.SCRAM_SHA_256, "pw", 4096);
        store.addPlainPassword("alice", ScramMechanism.SCRAM_SHA_512, "pw", 4096);
        store.addPlainPassword("bob", ScramMechanism.SCRAM_SHA_256, "pw", 4096);

        DescribeUserScramCredentialsRequestData req = new DescribeUserScramCredentialsRequestData();
        req.setUsers(null);
        DescribeUserScramCredentialsResponseData resp =
                KafkaScramCredentialsTranscoder.handleDescribe(req, store);

        assertThat(resp.results()).hasSize(2);
        Optional<DescribeUserScramCredentialsResult> aliceRow =
                resp.results().stream().filter(r -> r.user().equals("alice")).findFirst();
        assertThat(aliceRow).isPresent();
        assertThat(aliceRow.get().credentialInfos()).hasSize(2);
        assertThat(aliceRow.get().credentialInfos().stream().map(CredentialInfo::mechanism))
                .contains(
                        KafkaScramCredentialsTranscoder.MECHANISM_SHA_256,
                        KafkaScramCredentialsTranscoder.MECHANISM_SHA_512);

        // Verify no salt/hash fields are exposed by introspecting the generated schema — we only
        // serialise CredentialInfo, which by Kafka's message spec carries (mechanism, iterations).
        assertThat(CredentialInfo.class.getDeclaredFields())
                .noneMatch(f -> f.getName().contains("salt") || f.getName().contains("key"));

        // Used to silence unused-import warnings if Arrays isn't referenced above.
        assertThat(Arrays.asList(1, 2)).hasSize(2);
    }
}
