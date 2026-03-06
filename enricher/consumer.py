"""
Enricher Agent — Silver → Gold

Consumes: logs.silver.events.v1
Produces: logs.gold.assessed-events.v1
Dead letter: logs.dead.enrich-errors.v1

Runs the full LangGraph pipeline:
  enrich → score → reason (Ollama LLM) → route
"""
from __future__ import annotations
import os
import json
import time
import signal
import logging
from rich.console import Console
from confluent_kafka import KafkaError

from shared.config import config
from shared.kafka_client import make_producer, make_consumer, produce_message, produce_dead_letter
from shared.models import SilverEvent, AgentState, GoldEvent
from enricher.agent.graph import build_graph

console = Console()
running = True

SOURCE_TOPIC = config.kafka.topic_silver

def shutdown(sig, frame):
    global running
    console.print("\n[dim]Enricher agent shutting down...[/dim]")
    running = False

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

def run():
    agent = build_graph()
    consumer = make_consumer(group_id=config.kafka.group_enricher, topics=[SOURCE_TOPIC])
    producer = make_producer()

    console.print("[green]Enricher agent started[/green]")
    console.print(f"  consuming: [cyan]{SOURCE_TOPIC}[/cyan]")
    console.print(f"  producing: [cyan]{config.kafka.topic_gold}[/cyan]")
    console.print(f"  model:     [cyan]{config.ollama.model}[/cyan]")
    console.print(f"  flink:     [cyan]running in parallel on same silver topic[/cyan]")

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
            t_start = time.time()

            try:
                silver_dict = json.loads(raw_str)
                silver = SilverEvent(**silver_dict)
            except Exception as e:
                console.print(f"[red]Silver parse error: {e}[/red]")
                produce_dead_letter(
                    producer=producer,
                    dead_topic=config.kafka.topic_dead_enrich,
                    original_message=raw_str,
                    error=str(e),
                    stage="enrich-parse",
                    source_topic=SOURCE_TOPIC
                )
                consumer.commit(msg)
                continue
          
            try:
                state = AgentState(silver=silver)
                result = agent.invoke(state)

                processing_ms = int((time.time() - t_start) * 1000)

                gold = GoldEvent(
                    silver=silver,
                    ip_reputation=result.get("ip_reputation"),
                    asset_context=result.get("asset_context"),
                    identity_context=result.get("identity_context"),
                    threat_score=result.get("threat_score", 0),
                    score_breakdown=result.get("score_breakdown", {}),
                    recommended_action=result.get("recommended_action"),
                    narrative=result.get("narrative"),
                    mitre_tactic=result.get("mitre_tactic"),
                    processing_time_ms=processing_ms,
                    routed=True,
                )

                produce_message(
                    producer=producer,
                    topic=config.kafka.topic_gold,
                    message=gold.model_dump(),
                    key=silver.id,
                )

                consumer.commit(msg)

                console.print(
                    f"[green]✓[/green] {silver.event_type} | "
                    f"score=[bold]{gold.threat_score}[/bold] | "
                    f"action=[cyan]{gold.recommended_action}[/cyan] | "
                    f"{processing_ms}ms"
                )
            except Exception as e:
                console.print(f"[red]Enrichment error for {silver.id}: {e}[/red]")
                produce_dead_letter(
                    producer=producer,
                    dead_topic=config.kafka.topic_dead_enrich,
                    original_message=raw_str,
                    error=str(e),
                    stage="enrich",
                    source_topic=SOURCE_TOPIC
                )
                consumer.commit(msg)
    finally:
        producer.flush()
        consumer.close()
        console.print("[dim]Enricher agent stopped.[/dim]")

if __name__ == "__main__":
    run()