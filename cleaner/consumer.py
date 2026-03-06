# cleaner/consumer.py
"""
Cleaner Agent — Bronze → Silver

Consumes: logs.bronze.events.v1
Produces: logs.silver.events.v1
Dead letter: logs.dead.parse-errors.v1, logs.dead.clean-errors.v1

Pure Python — no LLM, no external API calls.
Fast, deterministic, fully unit-testable.
"""
from __future__ import annotations
import json
import signal
from rich.console import Console
from confluent_kafka import KafkaError

from shared.config import config
from shared.kafka_client import make_producer, make_consumer, produce_message, produce_dead_letter
from shared.models import BronzeEvent
from cleaner.transforms import clean_event

console = Console()
running = True


def shutdown(sig, frame):
    global running
    console.print("\n[dim]Cleaner agent shutting down...[/dim]")
    running = False


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def run():
    consumer = make_consumer(
        group_id=config.kafka.group_cleaner,
        topics=[config.kafka.topic_bronze],
    )
    producer = make_producer()

    console.print(f"[green]Cleaner agent started[/green]")
    console.print(f"  consuming: [cyan]{config.kafka.topic_bronze}[/cyan]")
    console.print(f"  producing: [cyan]{config.kafka.topic_silver}[/cyan]")

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    console.print(f"[red]Kafka error: {msg.error()}[/red]")
                continue

            raw_str = msg.value().decode("utf-8")

            # ── Stage 1: Parse ──────────────────────────────────────────
            try:
                raw_dict  = json.loads(raw_str)
                bronze    = BronzeEvent(**raw_dict)
            except Exception as e:
                console.print(f"[red]Parse error: {e}[/red]")
                produce_dead_letter(
                    producer, config.kafka.topic_dead_parse,
                    raw_str, str(e), "parse", config.kafka.topic_bronze,
                )
                consumer.commit(msg)
                continue

            # ── Stage 2: Clean ──────────────────────────────────────────
            try:
                silver = clean_event(bronze)
            except Exception as e:
                console.print(f"[red]Clean error for {bronze.id}: {e}[/red]")
                produce_dead_letter(
                    producer, config.kafka.topic_dead_clean,
                    raw_str, str(e), "clean", config.kafka.topic_bronze,
                )
                consumer.commit(msg)
                continue

            # ── Produce to silver ───────────────────────────────────────
            produce_message(
                producer,
                config.kafka.topic_silver,
                silver.model_dump(),
                key=silver.id,
            )

            consumer.commit(msg)

            if silver.cleaning_warnings:
                console.print(f"[yellow]{silver.id} cleaned with warnings: {silver.cleaning_warnings}[/yellow]")
            else:
                console.print(f"[dim]cleaned {silver.id} ({silver.event_type})[/dim]")

    finally:
        producer.flush()
        consumer.close()
        console.print("[dim]Cleaner agent stopped.[/dim]")


if __name__ == "__main__":
    run()