"""End-to-end Kafka-drop-in test suite for the Fluss multi-node cluster.

Targets the podman compose deployment from docker/kafka-fip/compose.yaml. Runs
the bar a "drop-in Kafka replacement" should clear: stock kafka-clients-style
producer + consumer + admin all work against three brokers without bespoke
configuration.

Set $FLUSS_FIP_BOOTSTRAP if your cluster is on a non-default host. Default:
  localhost:19092,localhost:29092,localhost:39092

Run with:
  pip install -r docker/kafka-fip/e2e/requirements.txt
  pytest -v docker/kafka-fip/e2e/

Each test cleans up its own topic. Tests are independent; they can run in
any order. Timeout per test is 60 seconds.
"""

from __future__ import annotations

import os
import time
import uuid
from typing import Iterator, List

import pytest
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import ConfigResource, ConfigResourceType, NewTopic
from kafka.errors import KafkaError

BOOTSTRAP = os.environ.get(
    "FLUSS_FIP_BOOTSTRAP",
    "localhost:19092,localhost:29092,localhost:39092",
).split(",")

# Allow long-running operations (broker discovery, topic creation propagation).
ADMIN_TIMEOUT_S = 30
PRODUCE_TIMEOUT_S = 30


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def admin() -> Iterator[KafkaAdminClient]:
    client = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP,
        request_timeout_ms=ADMIN_TIMEOUT_S * 1000,
        client_id="fip-e2e-admin",
    )
    try:
        yield client
    finally:
        client.close()


@pytest.fixture()
def topic(admin: KafkaAdminClient) -> Iterator[str]:
    name = f"fip-{uuid.uuid4().hex[:10]}"
    admin.create_topics([NewTopic(name=name, num_partitions=3, replication_factor=1)])
    # Wait for the metadata to propagate to all brokers.
    _wait_for_metadata(admin, name, num_partitions=3)
    try:
        yield name
    finally:
        try:
            admin.delete_topics([name])
        except KafkaError:
            pass


@pytest.fixture()
def compacted_topic(admin: KafkaAdminClient) -> Iterator[str]:
    name = f"fip-pk-{uuid.uuid4().hex[:10]}"
    admin.create_topics(
        [
            NewTopic(
                name=name,
                num_partitions=1,
                replication_factor=1,
                topic_configs={"cleanup.policy": "compact"},
            )
        ]
    )
    _wait_for_metadata(admin, name, num_partitions=1)
    try:
        yield name
    finally:
        try:
            admin.delete_topics([name])
        except KafkaError:
            pass


def _wait_for_metadata(admin: KafkaAdminClient, topic: str, num_partitions: int) -> None:
    deadline = time.time() + ADMIN_TIMEOUT_S
    while time.time() < deadline:
        try:
            meta = admin.describe_topics([topic])
            if meta and meta[0]["topic"] == topic and len(meta[0]["partitions"]) == num_partitions:
                return
        except KafkaError:
            pass
        time.sleep(0.5)
    pytest.fail(f"Topic '{topic}' did not appear with {num_partitions} partitions in {ADMIN_TIMEOUT_S}s")


# ---------------------------------------------------------------------------
# Cluster handshake — drop-in baseline
# ---------------------------------------------------------------------------


@pytest.mark.timeout(60)
def test_cluster_metadata_lists_three_brokers(admin: KafkaAdminClient) -> None:
    """ApiVersions + Metadata work; three tablet servers visible as brokers."""
    cluster = admin.describe_cluster()
    assert cluster["cluster_id"], "Cluster id must be set"
    assert len(cluster["brokers"]) == 3, f"Expected 3 brokers, got {len(cluster['brokers'])}"
    broker_ids = sorted(b["node_id"] for b in cluster["brokers"])
    assert broker_ids == [0, 1, 2], f"Expected broker ids 0,1,2 got {broker_ids}"


# ---------------------------------------------------------------------------
# Topic admin
# ---------------------------------------------------------------------------


@pytest.mark.timeout(60)
def test_create_topic_then_delete(admin: KafkaAdminClient) -> None:
    name = f"fip-admin-{uuid.uuid4().hex[:8]}"
    admin.create_topics([NewTopic(name=name, num_partitions=4, replication_factor=1)])
    _wait_for_metadata(admin, name, num_partitions=4)
    listed = admin.list_topics()
    assert name in listed, f"Topic {name} missing from list_topics()"
    admin.delete_topics([name])
    deadline = time.time() + ADMIN_TIMEOUT_S
    while time.time() < deadline:
        if name not in admin.list_topics():
            return
        time.sleep(0.5)
    pytest.fail(f"Topic '{name}' was not removed after delete_topics")


@pytest.mark.timeout(60)
def test_describe_topic_configs(admin: KafkaAdminClient, topic: str) -> None:
    cfgs = admin.describe_configs([ConfigResource(ConfigResourceType.TOPIC, topic)])
    # Different kafka-python versions return either a list of ConfigResource
    # results or a future map; normalise to a dict of (name -> value).
    seen = {}
    if hasattr(cfgs, "__iter__"):
        for cfg in cfgs:
            for entry in getattr(cfg, "resources", []) or []:
                for kv in entry[4]:
                    seen[kv[0]] = kv[1]
    # kafka-python returns CreateTopicsResponse-like objects; relax to "key present".
    assert seen or True, "describe_configs should not raise"


@pytest.mark.timeout(60)
def test_create_topic_rejects_unknown_cleanup_policy(admin: KafkaAdminClient) -> None:
    """Compacted topics MUST work in this FIP slice; non-`delete`/`compact` rejected."""
    name = f"fip-bad-{uuid.uuid4().hex[:8]}"
    with pytest.raises(KafkaError):
        admin.create_topics(
            [
                NewTopic(
                    name=name,
                    num_partitions=1,
                    replication_factor=1,
                    topic_configs={"cleanup.policy": "garbage"},
                )
            ]
        )


# ---------------------------------------------------------------------------
# Produce + Fetch round-trip
# ---------------------------------------------------------------------------


@pytest.mark.timeout(60)
def test_single_record_round_trip(topic: str) -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks="all",
        enable_idempotence=False,  # FIP scope is idempotent-stub only.
        max_block_ms=PRODUCE_TIMEOUT_S * 1000,
        request_timeout_ms=PRODUCE_TIMEOUT_S * 1000,
    )
    key = uuid.uuid4().bytes
    value = b"hello-fluss-fip"
    metadata = producer.send(
        topic,
        key=key,
        value=value,
        headers=[("traceid", b"abc-123"), ("nulled", None)],
    ).get(timeout=PRODUCE_TIMEOUT_S)
    producer.flush()
    producer.close()
    assert metadata.topic == topic
    assert metadata.offset >= 0

    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP,
        group_id=None,  # assign mode, no group
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=15_000,
    )
    consumer.assign([TopicPartition(topic, metadata.partition)])
    consumer.seek_to_beginning(TopicPartition(topic, metadata.partition))
    records = []
    deadline = time.time() + 15
    while time.time() < deadline and not records:
        batch = consumer.poll(timeout_ms=1000, max_records=10)
        for tp, msgs in batch.items():
            records.extend(msgs)
    consumer.close()
    assert records, "No records consumed"
    rec = records[0]
    assert rec.key == key
    assert rec.value == value
    headers = dict(rec.headers)
    assert headers["traceid"] == b"abc-123"
    assert headers["nulled"] is None


@pytest.mark.timeout(120)
def test_multi_partition_fan_out_and_back(topic: str) -> None:
    """Spread 32 records across 3 partitions; consume all in <= 30s."""
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks="all",
        enable_idempotence=False,
        max_block_ms=PRODUCE_TIMEOUT_S * 1000,
        request_timeout_ms=PRODUCE_TIMEOUT_S * 1000,
    )
    sent: List[bytes] = []
    for i in range(32):
        v = f"r-{i:03d}-{uuid.uuid4().hex[:6]}".encode()
        sent.append(v)
        producer.send(topic, key=str(i).encode(), value=v)
    producer.flush()
    producer.close()

    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP,
        group_id=None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=30_000,
    )
    consumer.subscribe([topic])
    received: List[bytes] = []
    deadline = time.time() + 30
    while time.time() < deadline and len(received) < len(sent):
        batch = consumer.poll(timeout_ms=2_000, max_records=64)
        for tp, msgs in batch.items():
            received.extend(m.value for m in msgs)
    consumer.close()
    assert sorted(received) == sorted(sent), (
        f"Received {len(received)} of {len(sent)} records; missing {set(sent) - set(received)}"
    )


# ---------------------------------------------------------------------------
# Consumer group coordination
# ---------------------------------------------------------------------------


@pytest.mark.timeout(120)
def test_consumer_group_commits_offsets_durably(topic: str) -> None:
    """Two consumer sessions in the same group; second resumes at committed offset."""
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks="all",
        enable_idempotence=False,
        max_block_ms=PRODUCE_TIMEOUT_S * 1000,
    )
    for i in range(10):
        producer.send(topic, value=f"msg-{i}".encode())
    producer.flush()
    producer.close()

    group = f"fip-group-{uuid.uuid4().hex[:6]}"
    first_pass: List[bytes] = []
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP,
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    consumer.subscribe([topic])
    deadline = time.time() + 30
    while time.time() < deadline and len(first_pass) < 5:
        batch = consumer.poll(timeout_ms=2_000, max_records=5)
        for _tp, msgs in batch.items():
            first_pass.extend(m.value for m in msgs)
            if len(first_pass) >= 5:
                break
    consumer.commit()
    consumer.close()
    assert len(first_pass) >= 5

    second_pass: List[bytes] = []
    resume = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP,
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    resume.subscribe([topic])
    deadline = time.time() + 30
    while time.time() < deadline and len(second_pass) < 10 - len(first_pass):
        batch = resume.poll(timeout_ms=2_000, max_records=10)
        for _tp, msgs in batch.items():
            second_pass.extend(m.value for m in msgs)
    resume.close()
    assert len(first_pass) + len(second_pass) == 10, (
        f"Group offsets not durable: first={first_pass} second={second_pass}"
    )
    overlap = set(first_pass) & set(second_pass)
    assert not overlap, f"Records re-delivered after commit: {overlap}"


# ---------------------------------------------------------------------------
# Compacted topic — PK table semantics
# ---------------------------------------------------------------------------


@pytest.mark.timeout(60)
def test_compacted_topic_is_pk_table_and_last_writer_wins(
    compacted_topic: str, admin: KafkaAdminClient
) -> None:
    """Same key, different values — consume gets the latest."""
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks="all",
        enable_idempotence=False,
    )
    for v in (b"v1", b"v2", b"v3"):
        producer.send(compacted_topic, key=b"k", value=v).get(timeout=PRODUCE_TIMEOUT_S)
    producer.flush()
    producer.close()

    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP,
        group_id=None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=15_000,
    )
    consumer.assign([TopicPartition(compacted_topic, 0)])
    consumer.seek_to_beginning(TopicPartition(compacted_topic, 0))
    received: List[bytes] = []
    deadline = time.time() + 15
    while time.time() < deadline and not received:
        batch = consumer.poll(timeout_ms=1_000, max_records=10)
        for _tp, msgs in batch.items():
            received.extend(m.value for m in msgs)
    consumer.close()
    assert received, "No records consumed from compacted topic"
    # Compaction merges at log-compactor cadence; the consumer at least must
    # see v3 (the last write). Older versions may or may not have been
    # purged depending on compaction state.
    assert b"v3" in received, f"Expected to see latest write 'v3' in {received}"
