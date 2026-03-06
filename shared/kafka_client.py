from __future__ import annotations
import json
from datetime import datetime, timezone
from confluent_kafka import Producer, Consumer
from shared.config import config
from shared.models import DeadLetterEvent


def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": config.kafka.bootstrap_servers,
        "acks":              "all",
        "retries":           5,
        "retry.backoff.ms":  500,
    })


def make_consumer(group_id: str, topics: list[str]) -> Consumer:
    consumer = Consumer({
        "bootstrap.servers":  config.kafka.bootstrap_servers,
        "group.id":           group_id,
        "auto.offset.reset":  config.kafka.auto_offset_reset,
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000,     # 5 min — LLM calls can be slow
    })
    consumer.subscribe(topics)
    return consumer


def produce_message(producer: Producer, topic: str, message: dict, key: str | None = None):
    producer.produce(
        topic=topic,
        key=key.encode("utf-8") if key else None,
        value=json.dumps(message).encode("utf-8"),
        callback=_delivery_callback,
    )
    producer.poll(0)


def produce_dead_letter(
    producer: Producer,
    dead_topic: str,
    original_message: str,
    error: str,
    stage: str,
    source_topic: str,
):
    dead = DeadLetterEvent(
        original_message=original_message,
        error=error,
        stage=stage,
        timestamp=datetime.now(timezone.utc).isoformat(),
        topic=source_topic,
    )
    produce_message(producer, dead_topic, dead.model_dump())


def _delivery_callback(err, msg):
    if err:
        print(f"[kafka] delivery failed: {err}")