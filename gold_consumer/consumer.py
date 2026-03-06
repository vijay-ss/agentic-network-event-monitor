"""
Gold Consumer — Storage sink for individually enriched events.

Reads from the gold Kafka topic and writes every assessed event
to both Elasticsearch and PostgreSQL. This is the missing link that populates
the two storage layers that Kibana and Superset query.

Consumes:  logs.gold.assessed-events.v1
Writes to: Elasticsearch → security-events index
           PostgreSQL    → events table

Data flow context:
  enricher-agent produces gold events to Kafka.
  Kafka Connect (connectors.sh) independently archives gold to MinIO.
  This service handles the hot (ES) and warm (Postgres) storage layers.
  All three run in parallel — no coordination needed.

Consumer group: security.gold-consumer.v1
  Separate from the enricher group so this service tracks its own offsets
  and can be restarted or replayed independently.
"""
from __future__ import annotations
import json
import signal
import time

import psycopg2
from elasticsearch import Elasticsearch
from confluent_kafka import KafkaError
from rich.console import Console

from shared.config import config
from shared.kafka_client import make_consumer
from shared.models import GoldEvent

console = Console()
running = True

CONSUMER_GROUP = "security.gold-consumer.v1"


def shutdown(sig, frame):
    global running
    console.print("\n[dim]Gold consumer shutting down...[/dim]")
    running = False


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def make_es_client() -> Elasticsearch:
    """
    Creates an Elasticsearch client pointed at the configured host.
    Security is disabled for local dev (xpack.security.enabled: false in compose).
    For production: add api_key or http_auth parameters here.
    """
    return Elasticsearch(
        hosts=[config.elastic.host],
        retry_on_timeout=True,
        max_retries=3,
    )


def write_to_elasticsearch(es: Elasticsearch, gold: GoldEvent) -> None:
    """
    Writes a gold event to the security-events Elasticsearch index.

    Uses the silver event ID as the document ID so re-processing the same
    event is idempotent — a retry won't create duplicate documents.

    The document is the full GoldEvent serialized to a dict. ES field types
    are enforced by the explicit mapping created by elasticsearch-init.
    """
    doc = gold.model_dump()

    es.index(
        index=config.elastic.index,
        id=gold.silver.id,
        document=doc,
    )


def make_pg_connection():
    """
    Creates a PostgreSQL connection using the DSN from shared config.
    Autocommit is off — each event is committed individually so a crash
    mid-batch doesn't leave partial writes.
    """
    conn = psycopg2.connect(config.postgres.dsn)
    conn.autocommit = False
    return conn


def write_to_postgres(conn, gold: GoldEvent) -> None:
    """
    Inserts a gold event into the events table.

    Uses INSERT ... ON CONFLICT DO NOTHING so replaying the Kafka topic
    (e.g. after a consumer restart with auto.offset.reset=earliest) doesn't
    create duplicate rows. The silver_id column has a unique constraint for this.

    Flattens the nested GoldEvent/SilverEvent structure into the denormalized
    events table schema defined in config/postgres/init.sql.
    """
    s = gold.silver
    pv = s.policy_violation

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO events (
                silver_id, original_id,
                event_time, ingestion_time, processing_time_ms,
                event_type, severity, action, protocol, application,
                connection_state, response_code, rule_id,
                source_ip, destination_ip, destination_domain,
                source_port, destination_port,
                bytes_sent, bytes_received, packet_count, duration_ms,
                tcp_flags, user_name, device_name,
                source_type, pipeline, collector,
                network_segment, security_zone, geo_location,
                vlan_id, asset_tags, correlation_id,
                process_name, process_id, cpu_percent, memory_percent,
                policy_name, policy_violation_type, policy_severity,
                threat_score, recommended_action, mitre_tactic, narrative,
                score_breakdown, ip_reputation, asset_context, identity_context,
                cleaning_warnings
            ) VALUES (
                %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s
            )
            ON CONFLICT (silver_id) DO NOTHING
        """, (
            s.id, s.original_id,
            s.event_time, s.ingestion_time, gold.processing_time_ms,
            s.event_type, s.severity.value if s.severity else None,
            s.action.value if s.action else None,
            s.protocol, s.application,
            s.connection_state, s.response_code, s.rule_id,
            s.source_ip, s.destination_ip, s.destination_domain,
            s.source_port, s.destination_port,
            s.bytes_sent, s.bytes_received, s.packet_count, s.duration_ms,
            s.tcp_flags, s.user, s.device_name,
            s.source_type, s.pipeline, s.collector,
            s.network_segment, s.security_zone, s.geo_location,
            s.vlan_id, s.asset_tags, s.correlation_id,
            s.process_name, s.process_id, s.cpu_percent, s.memory_percent,
            pv.policy_name if pv else None,
            pv.violation_type if pv else None,
            pv.policy_severity if pv else None,
            gold.threat_score,
            gold.recommended_action.value if gold.recommended_action else None,
            gold.mitre_tactic, gold.narrative,
            json.dumps(gold.score_breakdown),
            json.dumps(gold.ip_reputation.model_dump() if gold.ip_reputation else None),
            json.dumps(gold.asset_context.model_dump() if gold.asset_context else None),
            json.dumps(gold.identity_context.model_dump() if gold.identity_context else None),
            s.cleaning_warnings,
        ))
    conn.commit()


def run():
    es  = make_es_client()
    pg  = make_pg_connection()
    consumer = make_consumer(
        group_id=CONSUMER_GROUP,
        topics=[config.kafka.topic_gold],
    )

    console.print("[green]Gold consumer started[/green]")
    console.print(f"  consuming:  [cyan]{config.kafka.topic_gold}[/cyan]")
    console.print(f"  → ES index: [cyan]{config.elastic.index}[/cyan]")
    console.print(f"  → PG table: [cyan]events[/cyan]")

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
                gold = GoldEvent(**json.loads(raw_str))
            except Exception as e:
                console.print(f"[red]Parse error: {e}[/red]")
                consumer.commit(msg)
                continue

            es_ok = pg_ok = False

            try:
                write_to_elasticsearch(es, gold)
                es_ok = True
            except Exception as e:
                console.print(f"[red]ES write error for {gold.silver.id}: {e}[/red]")

            try:
                write_to_postgres(pg, gold)
                pg_ok = True
            except Exception as e:
                console.print(f"[red]PG write error for {gold.silver.id}: {e}[/red]")
                try:
                    pg = make_pg_connection()
                except Exception:
                    pass

            consumer.commit(msg)

            elapsed_ms = int((time.time() - t_start) * 1000)
            status = "[green]✓[/green]" if (es_ok and pg_ok) else "[yellow]~[/yellow]"
            console.print(
                f"{status} {gold.silver.event_type} | "
                f"score=[bold]{gold.threat_score}[/bold] | "
                f"ES={'✓' if es_ok else '✗'} PG={'✓' if pg_ok else '✗'} | "
                f"{elapsed_ms}ms"
            )

    finally:
        consumer.close()
        pg.close()
        console.print("[dim]Gold consumer stopped.[/dim]")


if __name__ == "__main__":
    run()