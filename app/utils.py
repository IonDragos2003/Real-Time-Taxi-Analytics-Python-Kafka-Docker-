"""
Shared utilities used across producer, processor, and API.
"""

import json
from datetime import datetime

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from app.config import (
    KAFKA_BOOTSTRAP,
    RAW_TOPIC, CLEANED_TOPIC, DLQ_TOPIC, AGGREGATES_TOPIC,
    RAW_PARTITIONS, CLEANED_PARTITIONS, DLQ_PARTITIONS, AGGREGATES_PARTITIONS,
)


# =============================================================================
# Serializers
# =============================================================================

def serialize_value(v: dict) -> bytes:
    """JSON-encode a dict to bytes. Handles non-serialisable types (e.g. Timestamps)."""
    return json.dumps(v, default=str).encode("utf-8")


def serialize_key(k) -> bytes | None:
    """Encode a key to bytes. Returns None if key is None."""
    return str(k).encode("utf-8") if k is not None else None


# =============================================================================
# Event builder
# =============================================================================

def row_to_event(row, row_id: int) -> dict:
    """
    Convert a pandas Series (one parquet row) to a Kafka event dict.
    Adds a ride_id and an event_ts (wall-clock time of production).
    """
    event = row.to_dict()
    if "ride_id" not in event:
        event["ride_id"] = str(row_id)
    event["event_ts"] = datetime.utcnow().isoformat() + "Z"
    return event


# =============================================================================
# Kafka factory + topic setup
# =============================================================================

def setup_topics() -> None:
    """
    Create all pipeline topics with the correct partition counts.
    Safe to call on every startup — skips topics that already exist.
    """
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)

    topics = [
        NewTopic(RAW_TOPIC,        num_partitions=RAW_PARTITIONS,        replication_factor=1),
        NewTopic(CLEANED_TOPIC,    num_partitions=CLEANED_PARTITIONS,    replication_factor=1),
        NewTopic(DLQ_TOPIC,        num_partitions=DLQ_PARTITIONS,        replication_factor=1),
        NewTopic(AGGREGATES_TOPIC, num_partitions=AGGREGATES_PARTITIONS, replication_factor=1,
                 topic_configs={"cleanup.policy": "compact"}),
    ]

    for topic in topics:
        try:
            admin.create_topics([topic])
            print(f"[setup] Created topic '{topic.name}' ({topic.num_partitions} partition(s))")
        except TopicAlreadyExistsError:
            print(f"[setup] Topic '{topic.name}' already exists — skipping")

    admin.close()


def make_producer() -> KafkaProducer:
    """Create a KafkaProducer with the shared serializers."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=serialize_value,
        key_serializer=serialize_key,
    )


def make_consumer(topic: str, group_id: str) -> KafkaConsumer:
    """Create a KafkaConsumer for the given topic and consumer group."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
