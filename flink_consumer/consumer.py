"""
Flink Consumer — Storage sink for Flink aggregated events.

Reads from all three Flink aggregated Kafka topics, builds an
enrichment_summary by joining against a local in-memory cache of recent gold
events (populated from the gold Kafka topic), then writes to Elasticsearch
and PostgreSQL.

Consumes:
  - logs.aggregated.windowed-events.v1    (velocity aggregations)
  - logs.aggregated.correlated-events.v1  (CEP attack chains)
  - logs.aggregated.baseline-alerts.v1    (baseline deviation alerts)
  - logs.gold.assessed-events.v1          (read-only, for enrichment lookup)

Writes to:
  - Elasticsearch → security-aggregations index
  - PostgreSQL    → aggregations table

── How the enrichment join works ────────────────────────────────────────────
Two Kafka consumers run in separate threads:

  Thread 1 (gold cache thread):
    Reads logs.gold.assessed-events.v1 continuously.
    Stores each gold event in an in-memory dict keyed by:
      source_ip:event_type → list of GoldEvents with timestamps
    Each entry has a TTL of CACHE_TTL_MINUTES so memory is bounded.
    Consumer group: security.flink-enrichment-cache.v1
    (separate group — does not interfere with gold_consumer offsets)

  Thread 2 (aggregation consumer — main thread):
    Reads the three aggregated topics.
    For each Flink event, looks up matching gold events from the cache
    using source_ip + event_type + time window overlap.
    Builds an EnrichmentSummary from the matching gold events.
    Writes the enriched aggregation to ES and Postgres.
    Consumer group: security.flink-consumer.v1

This avoids reading back from Elasticsearch and keeps the join in Kafka,
which is the correct place for it in a streaming pipeline.
────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations
import json
import signal
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Optional

import psycopg2
from elasticsearch import Elasticsearch
from confluent_kafka import KafkaError
from rich.console import Console

from shared.config import config
from shared.kafka_client import make_consumer
from shared.models import GoldEvent, AggregationEvent, EnrichmentSummary

console = Console()
running = True

CACHE_TTL_MINUTES = int(config.flink.cep_window_minutes * 3)

GoldCache = defaultdict(list)
CACHE_LOCK = threading.Lock()


def shutdown(sig, frame):
    global running
    console.print("\n[dim]Flink consumer shutting down...[/dim]")
    running = False


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def run_gold_cache_thread():
    """
    Thread 1: Continuously reads the gold Kafka topic and populates
    GoldCache so the main aggregation consumer can do enrichment lookups
    without any network round-trips to Elasticsearch.

    Uses its own consumer group so it tracks offsets independently from
    gold_consumer and never competes with it for messages.
    """
    consumer = make_consumer(
        group_id="security.flink-enrichment-cache.v1",
        topics=[config.kafka.topic_gold],
    )

    console.print("[dim]Gold cache thread started[/dim]")

    while running:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                console.print(f"[red]Cache thread Kafka error: {msg.error()}[/red]")
            continue

        try:
            gold = GoldEvent(**json.loads(msg.value().decode("utf-8")))
            key  = f"{gold.silver.source_ip}:{gold.silver.event_type}"

            event_dt = datetime.fromisoformat(
                gold.silver.event_time.replace("Z", "+00:00")
            )

            with CACHE_LOCK:
                GoldCache[key].append((event_dt, gold))
                cutoff = datetime.now(timezone.utc) - timedelta(minutes=CACHE_TTL_MINUTES)
                GoldCache[key] = [
                    (dt, g) for dt, g in GoldCache[key] if dt >= cutoff
                ]

            consumer.commit(msg)

        except Exception as e:
            console.print(f"[red]Cache thread parse error: {e}[/red]")
            consumer.commit(msg)

    consumer.close()
    console.print("[dim]Gold cache thread stopped.[/dim]")


def build_enrichment_summary(agg: AggregationEvent) -> Optional[EnrichmentSummary]:
    """
    Looks up gold events from GoldCache that match this aggregation's
    source_ip, event_type, and time window, then builds an EnrichmentSummary.

    The summary contains the max/avg threat score, the highest-scoring
    event's recommended_action and narrative, and the IP/asset/identity
    context from the enricher agent.

    Returns None if no matching gold events are found in the cache —
    this is normal for very recent events before the cache warms up.
    """
    if not agg.source_ip or not agg.source_event_type:
        return None

    key = f"{agg.source_ip}:{agg.source_event_type}"

    try:
        window_start = datetime.fromisoformat(
            (agg.first_seen or agg.event_time or "").replace("Z", "+00:00")
        )
        window_end = datetime.fromisoformat(
            (agg.last_seen or agg.event_time or "").replace("Z", "+00:00")
        )
    except (ValueError, TypeError):
        return None

    with CACHE_LOCK:
        candidates = [
            g for dt, g in GoldCache.get(key, [])
            if window_start <= dt <= window_end
        ]

    if not candidates:
        return None

    candidates.sort(key=lambda g: g.threat_score, reverse=True)
    top = candidates[0]

    scores = [g.threat_score for g in candidates]

    return EnrichmentSummary(
        max_threat_score=max(scores),
        avg_threat_score=int(sum(scores) / len(scores)),
        recommended_action=top.recommended_action.value if top.recommended_action else None,
        mitre_tactic=top.mitre_tactic,
        narrative=top.narrative,
        ip_reputation=top.ip_reputation,
        asset_context=top.asset_context,
        identity_context=top.identity_context,
        source_event_ids=[g.silver.id for g in candidates],
    )


def topic_to_aggregation_type(topic: str) -> str:
    """
    Maps the source Kafka topic to the aggregation_type string stored in
    Postgres and Elasticsearch. Makes it easy to filter by type in queries.
    """
    if "windowed" in topic:
        return "velocity"
    if "correlated" in topic:
        return "cep_chain"
    if "baseline" in topic:
        return "baseline"
    return "unknown"


def make_es_client() -> Elasticsearch:
    return Elasticsearch(
        hosts=[config.elastic.host],
        retry_on_timeout=True,
        max_retries=3,
    )


def write_to_elasticsearch(es: Elasticsearch, agg: AggregationEvent) -> None:
    """
    Writes an enriched aggregation event to the security-aggregations index.
    Uses the Flink-generated ID as the document ID for idempotency.
    """
    es.index(
        index=config.elastic.index_aggregations,
        id=agg.id,
        document=agg.model_dump(),
    )


def make_pg_connection():
    conn = psycopg2.connect(config.postgres.dsn)
    conn.autocommit = False
    return conn


def write_to_postgres(conn, agg: AggregationEvent) -> None:
    """
    Inserts an enriched aggregation into the aggregations table.
    ON CONFLICT DO NOTHING makes this idempotent on the flink_id column.
    """
    es = agg.enrichment_summary
    bs = agg.baseline_stats

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO aggregations (
                flink_id, aggregation_type,
                source_ip, source_event_type,
                event_time, first_seen, last_seen,
                window_minutes, description,
                event_count, unique_dest_ips, unique_dest_ports, total_bytes_sent,
                rolling_average, baseline_multiplier, windows_in_baseline,
                pattern, chain,
                max_threat_score, avg_threat_score,
                recommended_action, mitre_tactic, narrative,
                ip_reputation, asset_context, identity_context,
                source_event_ids
            ) VALUES (
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s
            )
            ON CONFLICT (flink_id) DO NOTHING
        """, (
            agg.id, agg.aggregation_type,
            agg.source_ip, agg.source_event_type,
            agg.event_time, agg.first_seen, agg.last_seen,
            agg.window_minutes, agg.description,
            agg.event_count, agg.unique_dest_ips,
            agg.unique_dest_ports, agg.total_bytes_sent,
            bs.rolling_average if bs else None,
            bs.multiplier if bs else None,
            bs.windows_in_baseline if bs else None,
            agg.pattern,
            json.dumps(agg.chain) if agg.chain else None,
            es.max_threat_score if es else None,
            es.avg_threat_score if es else None,
            es.recommended_action if es else None,
            es.mitre_tactic if es else None,
            es.narrative if es else None,
            json.dumps(es.ip_reputation.model_dump() if es and es.ip_reputation else None),
            json.dumps(es.asset_context.model_dump() if es and es.asset_context else None),
            json.dumps(es.identity_context.model_dump() if es and es.identity_context else None),
            es.source_event_ids if es else [],
        ))
    conn.commit()


def run():
    cache_thread = threading.Thread(target=run_gold_cache_thread, daemon=True)
    cache_thread.start()

    console.print("[dim]Warming gold cache (5s)...[/dim]")
    time.sleep(5)

    es  = make_es_client()
    pg  = make_pg_connection()

    consumer = make_consumer(
        group_id=config.kafka.group_flink,
        topics=[
            config.kafka.topic_aggregated_windowed,
            config.kafka.topic_aggregated_correlated,
            config.kafka.topic_aggregated_baseline,
        ],
    )

    console.print("[green]Flink consumer started[/green]")
    console.print(f"  consuming: [cyan]windowed + correlated + baseline[/cyan]")
    console.print(f"  → ES index: [cyan]{config.elastic.index_aggregations}[/cyan]")
    console.print(f"  → PG table: [cyan]aggregations[/cyan]")
    console.print(f"  cache TTL:  [cyan]{CACHE_TTL_MINUTES} minutes[/cyan]")

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    console.print(f"[red]Kafka error: {msg.error()}[/red]")
                continue

            raw_str  = msg.value().decode("utf-8")
            topic    = msg.topic()
            t_start  = time.time()

            try:
                raw_dict = json.loads(raw_str)
                raw_dict["aggregation_type"] = topic_to_aggregation_type(topic)
                agg = AggregationEvent(**raw_dict)
            except Exception as e:
                console.print(f"[red]Parse error on {topic}: {e}[/red]")
                consumer.commit(msg)
                continue

            agg.enrichment_summary = build_enrichment_summary(agg)
            enriched = agg.enrichment_summary is not None

            es_ok = pg_ok = False

            try:
                write_to_elasticsearch(es, agg)
                es_ok = True
            except Exception as e:
                console.print(f"[red]ES write error for {agg.id}: {e}[/red]")

            try:
                write_to_postgres(pg, agg)
                pg_ok = True
            except Exception as e:
                console.print(f"[red]PG write error for {agg.id}: {e}[/red]")
                try:
                    pg = make_pg_connection()
                except Exception:
                    pass

            consumer.commit(msg)

            elapsed_ms = int((time.time() - t_start) * 1000)
            status = "[green]✓[/green]" if (es_ok and pg_ok) else "[yellow]~[/yellow]"
            enrich_tag = "[cyan]enriched[/cyan]" if enriched else "[dim]no match[/dim]"
            console.print(
                f"{status} [{agg.aggregation_type}] "
                f"{agg.source_ip} | "
                f"count={agg.event_count or '—'} | "
                f"{enrich_tag} | "
                f"ES={'✓' if es_ok else '✗'} PG={'✓' if pg_ok else '✗'} | "
                f"{elapsed_ms}ms"
            )

    finally:
        consumer.close()
        pg.close()
        console.print("[dim]Flink consumer stopped.[/dim]")


if __name__ == "__main__":
    run()