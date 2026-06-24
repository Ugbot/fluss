#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
End-to-end Python test against the Fluss Kafka bolt-on.

Uses ``confluent-kafka-python`` (librdkafka under the hood — the same C
client that powers kcat) to drive produce / fetch / consumer-group paths.
A third-party client is a stronger portability check than kafka-clients
(Java) because librdkafka has its own protocol parser — if there were
subtle wire-bytes bugs only Java tolerated, these tests would fail.

Install deps::

    pip install confluent-kafka

Start a Fluss cluster with the Kafka listener enabled, then::

    python fluss_kafka_e2e.py 127.0.0.1:9092

Exit code is 0 on full success, 1 otherwise. Intended to be run from
CI or locally against a live cluster; not meant to manage Fluss's
lifecycle itself (see ``run_python_e2e.sh`` in the same directory for a
bash wrapper that does start / stop / run).
"""

import argparse
import random
import string
import sys
import time
import uuid
from typing import List

try:
    from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic
except ImportError:
    print("ERROR: confluent-kafka not installed. Run: pip install confluent-kafka")
    sys.exit(1)


def _rand_suffix(n: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


class FlussKafkaE2E:
    """Minimal e2e harness that exercises Fluss's Kafka wire protocol."""

    def __init__(self, bootstrap: str) -> None:
        self.bootstrap = bootstrap
        self.run_id = _rand_suffix()
        self.failures: List[str] = []

    # --- harness helpers ---

    def _log(self, msg: str) -> None:
        print(f"  {msg}")

    def _admin(self) -> AdminClient:
        return AdminClient({"bootstrap.servers": self.bootstrap})

    def _producer(self) -> Producer:
        return Producer(
            {
                "bootstrap.servers": self.bootstrap,
                "acks": "all",
                "message.timeout.ms": 15000,
                # librdkafka's idempotent path pokes at INIT_PRODUCER_ID;
                # stay non-idempotent so we exercise the plain-produce shape
                # the non-transactional Fluss bolt-on targets today.
                "enable.idempotence": False,
            }
        )

    def _consumer(self, group_id: str) -> Consumer:
        return Consumer(
            {
                "bootstrap.servers": self.bootstrap,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "session.timeout.ms": 10000,
            }
        )

    def _create_topic(self, topic: str, partitions: int = 1) -> None:
        admin = self._admin()
        result = admin.create_topics([NewTopic(topic, partitions, 1)])
        for t, fut in result.items():
            fut.result(timeout=20)

    # --- tests ---

    def test_list_topics(self) -> bool:
        topic = f"py_meta_{self.run_id}_{_rand_suffix(4)}"
        self._create_topic(topic)
        admin = self._admin()
        md = admin.list_topics(timeout=10)
        if topic not in md.topics:
            self.failures.append(f"list_topics: {topic} not in metadata")
            return False
        self._log(f"metadata lists {topic}: OK")
        return True

    def test_produce_consume_round_trip(self, count: int = 50) -> bool:
        topic = f"py_rt_{self.run_id}_{_rand_suffix(4)}"
        self._create_topic(topic)

        produced = [f"msg-{i}-{uuid.uuid4().hex[:6]}" for i in range(count)]
        delivered: List[str] = []
        errors: List[str] = []

        def on_delivery(err, msg):
            if err is not None:
                errors.append(str(err))
            else:
                delivered.append(msg.value().decode())

        prod = self._producer()
        for v in produced:
            prod.produce(topic, value=v.encode(), on_delivery=on_delivery)
        prod.flush(timeout=20)

        if errors:
            self.failures.append(f"produce errors: {errors[:3]}")
            return False
        if len(delivered) != count:
            self.failures.append(
                f"produce ack count mismatch: expected {count} acks, got {len(delivered)}"
            )
            return False
        self._log(f"produced {count} messages")

        consumer = self._consumer(f"rt-group-{self.run_id}-{_rand_suffix(4)}")
        try:
            consumer.subscribe([topic])
            received: List[str] = []
            deadline = time.time() + 20
            while time.time() < deadline and len(received) < count:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    self.failures.append(f"consume error: {msg.error()}")
                    return False
                received.append(msg.value().decode())
        finally:
            consumer.close()

        if set(received) != set(produced):
            missing = set(produced) - set(received)
            self.failures.append(
                f"consume mismatch: expected {count} got {len(received)}; "
                f"missing {len(missing)}"
            )
            return False
        self._log(f"consumed all {count} messages back: OK")
        return True

    def test_consumer_group_offset_commit(self) -> bool:
        topic = f"py_cg_{self.run_id}_{_rand_suffix(4)}"
        self._create_topic(topic)
        group_id = f"cg-{self.run_id}-{_rand_suffix(4)}"

        prod = self._producer()
        for i in range(10):
            prod.produce(topic, value=f"cg-{i}".encode())
        prod.flush(timeout=15)

        # First consumer reads 5, commits, closes.
        c1 = self._consumer(group_id)
        try:
            c1.subscribe([topic])
            first_batch: List[str] = []
            deadline = time.time() + 15
            while time.time() < deadline and len(first_batch) < 5:
                msg = c1.poll(1.0)
                if msg is None or msg.error():
                    continue
                first_batch.append(msg.value().decode())
                c1.commit(message=msg, asynchronous=False)
        finally:
            c1.close()

        if len(first_batch) < 5:
            self.failures.append(
                f"offset-commit: only read {len(first_batch)} before planned cutoff"
            )
            return False

        # Second consumer same group resumes where the first left off.
        c2 = self._consumer(group_id)
        try:
            c2.subscribe([topic])
            remaining: List[str] = []
            deadline = time.time() + 15
            while time.time() < deadline and len(remaining) < 5:
                msg = c2.poll(1.0)
                if msg is None or msg.error():
                    continue
                remaining.append(msg.value().decode())
        finally:
            c2.close()

        if len(remaining) != 5 or set(first_batch) & set(remaining):
            self.failures.append(
                f"offset-commit: resume overlap — first={first_batch} "
                f"remaining={remaining}"
            )
            return False
        self._log("consumer group resume after commit: OK")
        return True

    def test_list_offsets(self) -> bool:
        topic = f"py_lo_{self.run_id}_{_rand_suffix(4)}"
        self._create_topic(topic)
        prod = self._producer()
        for i in range(7):
            prod.produce(topic, value=f"lo-{i}".encode())
        prod.flush(timeout=15)

        consumer = self._consumer(f"lo-group-{self.run_id}-{_rand_suffix(4)}")
        try:
            tp = TopicPartition(topic, 0)
            low, high = consumer.get_watermark_offsets(tp, timeout=10)
        finally:
            consumer.close()

        if high < 7:
            self.failures.append(
                f"list_offsets: high watermark expected >=7, got {high} (low={low})"
            )
            return False
        self._log(f"watermarks low={low} high={high} (>= 7): OK")
        return True

    def test_headers_round_trip(self) -> bool:
        topic = f"py_hdr_{self.run_id}_{_rand_suffix(4)}"
        self._create_topic(topic)

        prod = self._producer()
        expected_headers = [("trace-id", b"abc-123"), ("tenant", b"acme")]
        prod.produce(
            topic,
            key=b"k1",
            value=b"headered-payload",
            headers=expected_headers,
        )
        prod.flush(timeout=15)

        consumer = self._consumer(f"hdr-group-{self.run_id}-{_rand_suffix(4)}")
        try:
            consumer.subscribe([topic])
            got = None
            deadline = time.time() + 15
            while time.time() < deadline and got is None:
                msg = consumer.poll(1.0)
                if msg is None or msg.error():
                    continue
                got = msg
        finally:
            consumer.close()

        if got is None:
            self.failures.append("headers: no message received")
            return False
        actual_headers = got.headers() or []
        as_dict = {k: v for k, v in actual_headers}
        for expected_key, expected_value in expected_headers:
            if as_dict.get(expected_key) != expected_value:
                self.failures.append(
                    f"headers: expected {expected_key}={expected_value!r}, "
                    f"got {actual_headers}"
                )
                return False
        self._log(f"headers round-tripped: {actual_headers}")
        return True

    def test_multi_partition_spread(self) -> bool:
        topic = f"py_mp_{self.run_id}_{_rand_suffix(4)}"
        self._create_topic(topic, partitions=3)

        prod = self._producer()
        for i in range(30):
            # Explicit partition steering — 10 msgs per partition.
            prod.produce(topic, key=f"k{i}".encode(), value=f"v{i}".encode(),
                         partition=i % 3)
        prod.flush(timeout=15)

        consumer = self._consumer(f"mp-group-{self.run_id}-{_rand_suffix(4)}")
        per_partition = {0: 0, 1: 0, 2: 0}
        try:
            consumer.subscribe([topic])
            total = 0
            deadline = time.time() + 20
            while time.time() < deadline and total < 30:
                msg = consumer.poll(1.0)
                if msg is None or msg.error():
                    continue
                per_partition[msg.partition()] = per_partition.get(msg.partition(), 0) + 1
                total += 1
        finally:
            consumer.close()

        if total != 30:
            self.failures.append(
                f"multi_partition: expected 30 msgs, got {total} "
                f"(per-partition {per_partition})"
            )
            return False
        if any(v != 10 for v in per_partition.values()):
            self.failures.append(
                f"multi_partition: uneven spread {per_partition}"
            )
            return False
        self._log(f"multi-partition: 10/10/10 across 3 partitions: OK")
        return True

    # --- entry point ---

    def run(self) -> int:
        tests = [
            ("list_topics", self.test_list_topics),
            ("produce_consume_round_trip", self.test_produce_consume_round_trip),
            ("consumer_group_offset_commit", self.test_consumer_group_offset_commit),
            ("list_offsets_watermarks", self.test_list_offsets),
            ("headers_round_trip", self.test_headers_round_trip),
            ("multi_partition_spread", self.test_multi_partition_spread),
        ]
        print(f"Fluss Kafka e2e — bootstrap={self.bootstrap} run={self.run_id}")
        passed = 0
        for name, fn in tests:
            print(f"[{name}]")
            try:
                ok = fn()
            except Exception as e:
                ok = False
                self.failures.append(f"{name}: {e}")
            status = "PASS" if ok else "FAIL"
            print(f"  {status}")
            if ok:
                passed += 1
        print()
        print(f"Summary: {passed}/{len(tests)} passed")
        if self.failures:
            print("Failures:")
            for f in self.failures:
                print(f"  - {f}")
            return 1
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "bootstrap",
        nargs="?",
        default="127.0.0.1:9092",
        help="Fluss Kafka listener bootstrap address (default 127.0.0.1:9092)",
    )
    args = parser.parse_args()
    return FlussKafkaE2E(args.bootstrap).run()


if __name__ == "__main__":
    sys.exit(main())
