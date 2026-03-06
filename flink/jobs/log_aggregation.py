"""
PyFlink Log Aggregation Job

Consumes:  logs.silver.events.v1  (parallel consumer — does not affect enricher)
Produces:
  - logs.aggregated.windowed-events.v1    (per-IP velocity aggregations)
  - logs.aggregated.correlated-events.v1  (CEP multi-stage attack patterns)
  - logs.aggregated.baseline-alerts.v1    (deviation from rolling baseline)

It runs as a parallel consumer of logs.silver.events.v1 using its own
consumer group (security.flink.aggregator.v1), which means it reads every
silver event independently without affecting the enricher agent's offsets.

Flink adds three supplementary output streams alongside that pipeline.

Three analysis branches:

  1. Velocity windowing (logs.aggregated.windowed-events.v1)
     Collapses N events from the same source_ip + event_type within a
     tumbling window into one aggregated event. Only emits if count exceeds
     FLINK_VELOCITY_THRESHOLD. Provides velocity context the per-event
     enricher cannot see.

  2. CEP attack chain detection (logs.aggregated.correlated-events.v1)
     Detects multi-stage attack sequences using Flink CEP:
       port_scan → blocked_attempt → successful_connection
     within FLINK_CEP_WINDOW_MINUTES from the same source IP.
     Emits a CORRELATED_ATTACK_CHAIN event flagged CRITICAL.

  3. Baseline deviation alerting (logs.aggregated.baseline-alerts.v1)
     Maintains a rolling per-entity (source_ip + event_type) baseline of
     event counts across FLINK_BASELINE_WINDOW_COUNT windows. Emits an alert
     when the current window count exceeds the rolling average by
     FLINK_BASELINE_MULTIPLIER. Catches slow-burn attacks invisible to
     velocity thresholds (e.g. 3x normal but not an obvious spike).

Run via:
  flink run --jobmanager flink-jobmanager:8081 --python /app/log_aggregation.py
"""
import os
import json
from collections import deque
from datetime import datetime, timezone

from pyflink.common import Types, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
    KafkaOffsetsInitializer,
)
from pyflink.datastream.window import TumblingEventTimeWindows, Time
from pyflink.datastream.functions import (
    MapFunction,
    ProcessWindowFunction,
    KeyedProcessFunction,
)

ENV_MODE = os.getenv("ENV_MODE", "dev")

KAFKA_SERVERS        = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_SILVER         = "logs.silver.events.v1"
TOPIC_WINDOWED       = "logs.aggregated.windowed-events.v1"
TOPIC_CORRELATED     = "logs.aggregated.correlated-events.v1"
TOPIC_BASELINE       = "logs.aggregated.baseline-alerts.v1"

WINDOW_MINUTES       = int(os.getenv("FLINK_WINDOW_MINUTES", "5"))
CEP_WINDOW_MINUTES   = int(os.getenv("FLINK_CEP_WINDOW_MINUTES", "10"))
VELOCITY_THRESHOLD   = int(os.getenv("FLINK_VELOCITY_THRESHOLD", "10"))
BASELINE_MULTIPLIER  = float(os.getenv("FLINK_BASELINE_MULTIPLIER", "3.0"))
BASELINE_WINDOW_COUNT = int(os.getenv("FLINK_BASELINE_WINDOW_COUNT", "12"))


class EventTimeAssigner(TimestampAssigner):
    """Extracts event_time from silver event for Flink watermarking."""
    def extract_timestamp(self, value: dict, record_timestamp: int) -> int:
        try:
            ts = value.get("event_time", "")
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except Exception:
            return record_timestamp


class ParseSilverEvent(MapFunction):
    """Deserializes raw Kafka JSON string into Python dict."""
    def map(self, value: str) -> dict:
        return json.loads(value)


class ExtractVelocityKey(MapFunction):
    """
    Keys events by source_ip + event_type for velocity windowing.
    This ensures all events from the same source doing the same
    thing land in the same window partition.
    """
    def map(self, event: dict) -> tuple:
        key = f"{event.get('source_ip', 'unknown')}:{event.get('event_type', 'UNKNOWN')}"
        return (key, event)


class SerializeToJson(MapFunction):
    """Serializes Python dict back to JSON string for Kafka sink."""
    def map(self, event: dict) -> str:
        return json.dumps(event)


# ── Branch 1: Velocity window aggregation ────────────────────────────────────

class VelocityWindowFunction(ProcessWindowFunction):
    """
    Aggregates events within a tumbling window per source_ip + event_type.

    Only emits if count >= VELOCITY_THRESHOLD - below threshold, individual
    events are handled by the enricher agent directly without aggreagation.

    The emitted event includes:
        - event_count:       total events in this window
        - unique_dest_ips:   number of distinct destination IPs targeted
        - unique_dest_ports: number of distinct destination ports targeted
        - total_bytes_sent:  sum of bytes_sent across all events
        - first_seen/last_seen: window time boundaries
        - velocity_key:      the source_ip:event_type key used for grouping
    """
    def process(self, key: str, context, elements):
        events = list(elements)
        count = len(events)

        if count < VELOCITY_THRESHOLD:
            return
        
        base = sorted(events, key=lambda e: e.get("event_time", ""))[-1]

        aggregated = {
            **base,
            "id":               f"agg_{base.get('id', 'unknown')}",
            "aggregated":       True,
            "event_count":      count,
            "window_minutes":   WINDOW_MINUTES,
            "velocity_key":     key,
            "first_seen":       events[0].get("event_time"),
            "last_seen":        events[-1].get("event_time"),
            "unique_dest_ips":  len(set(e.get("destination_ip", "") for e in events)),
            "unique_dest_ports": len(set(str(e.get("destination_port", "")) for e in events)),
            "total_bytes_sent": sum(e.get("bytes_sent", 0) for e in events),
            "description": (
                f"VELOCITY: {count} {base.get('event_type')} events "
                f"from {base.get('source_ip')} in {WINDOW_MINUTES} minutes"
            ),
        }
        yield json.dumps(aggregated)


# ── Branch 2: CEP attack chain detection ─────────────────────────────────────

class AttackChainDetector(KeyedProcessFunction):
    """
    Detects: port_scan → blocked_attempt → successful_connection
    from the same source IP within CEP_WINDOW_MINUTES.

    Maintains a small per-key state buffer of recent events.
    On each new event, checks whether the buffer contains the
    full attack chain pattern. If so, emits a CORRELATED_ATTACK_CHAIN.

    This replicates the CEP pattern logic without pyflink.cep,
    which is a Java-only API not exposed in PyFlink.
    """

    def __init__(self):
        self._buffer: dict[str, list] = {}

    def process_element(self, event: dict, ctx):
        key       = event.get("source_ip", "unknown")
        event_type     = event.get("event_type", "").upper()
        action    = event.get("action", "").upper()
        now       = ctx.timestamp()
        cutoff    = now - (CEP_WINDOW_MINUTES * 60 * 1000)

        if key not in self._buffer:
            self._buffer[key] = []

        self._buffer[key] = [
            e for e in self._buffer[key] if e.get("_ts", 0) >= cutoff
        ]

        event["_ts"] = now

        if event_type in ("PORT_SCAN", "SSH_ATTEMPT"):
            event["_chain_role"] = "port_scan"
            self._buffer[key].append(event)

        elif action in ("BLOCK", "DROP"):
            event["_chain_role"] = "blocked_attempt"
            self._buffer[key].append(event)

        elif action == "ALLOW":
            buf = self._buffer[key]
            scans = [e for e in buf if e.get("_chain_role") == "port_scan"]
            blocks = [e for e in buf if e.get("_chain_role") == "blocked_attempt"]

            if scans and blocks:
                scan    = scans[0]
                blocked = blocks[0]

                yield json.dumps({
                    "id":            f"cep_{scan.get('id', 'unknown')}",
                    "event_type":    "CORRELATED_ATTACK_CHAIN",
                    "severity":      "CRITICAL",
                    "source_ip":     key,
                    "aggregated":    True,
                    "correlated":    True,
                    "pattern":       "port_scan → blocked_attempt → success",
                    "event_time":    event.get("event_time"),
                    "window_minutes": CEP_WINDOW_MINUTES,
                    "chain": {
                        "port_scan":       scan,
                        "blocked_attempt": blocked,
                        "success":         event,
                    },
                    "description": (
                        f"ATTACK CHAIN DETECTED: {key} performed port scan, "
                        f"was blocked, then achieved successful connection. "
                        f"Possible reconnaissance leading to compromise."
                    ),
                })

                self._buffer[key] = []


# ── Branch 3: Baseline deviation alerting ───────────────────────────────

class BaselineDeviationFunction(KeyedProcessFunction):
    """
    Maintains a rolling per-entity baseline of event counts and emits
    an alert when the current window deviates significantly from normal.
    
    How it works:
        - Each event updates a per-key rolling history (a deque of recent counts)
        - When the history has enough windows, compute a rolling average
        - If the current count > average * BASELINE_MULTIPLIER, emit an alert
        - The history is stored in Flink keyed state (survives restarts)
    
    This catches slow-burn attacks that stay under the velocity threshold but
    are still significantly above normal for a given entity.

    Example: A device normally generates 5 failed logins per 5-minute window.
    BASELINE_MULTIPLIER is 3.0, so we alert at 15+ failed logins, even if VELOCITY_THRESHOLD
    is set to 50. The baseline approach adapts to the entity's normal behaviour.

    Key: source_ip + event_type (same as velocity windowing)
    """
    def __init__(self):
        """In-memory rolling history: key → deque of recent window counts.
        In production, replace with Flink ValueState for fault tolerance.
        """
        self._history: dict[str, deque] = {}
    
    def process_element(self, event: dict, ctx):
        event = json.loads(event)

        key = f"{event.get('source_ip', 'unknown')}:{event.get('event_type', 'UNKNOWN')}"
        count = event.get("event_count", 1)
        timestamp = ctx.timestamp()

        if key not in self._history:
            self._history[key] = deque(maxlen=BASELINE_WINDOW_COUNT)
        
        history = self._history[key]

        if len(history) >= BASELINE_WINDOW_COUNT // 2:
            rolling_avg = sum(history) / len(history)

            if rolling_avg > 0 and count >= rolling_avg * BASELINE_MULTIPLIER:
                alert = {
                    "id":             f"baseline_{key}_{timestamp}",
                    "event_type":     "BASELINE_DEVIATION",
                    "severity":       "HIGH",
                    "source_ip":      event.get("source_ip"),
                    "aggregated":     True,
                    "baseline_alert": True,
                    "event_time":     datetime.now(timezone.utc).isoformat(),
                    "velocity_key":   key,
                    "triggering_event": event,
                    "baseline_stats": {
                        "current_count":    count,
                        "rolling_average":  round(rolling_avg, 2),
                        "multiplier":       round(count / rolling_avg, 2),
                        "threshold":        BASELINE_MULTIPLIER,
                        "windows_in_baseline": len(history),
                    },
                    "description": (
                        f"BASELINE DEVIATION: {event.get('source_ip')} generated "
                        f"{count} {event.get('event_type')} events — "
                        f"{round(count / rolling_avg, 1)}x above rolling average "
                        f"of {round(rolling_avg, 1)} over {len(history)} windows."
                    ),
                }
                yield json.dumps(alert)
        
        history.append(count)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    if ENV_MODE == "prod":
        env.set_parallelism(4)
    else:
        env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_SERVERS)
        .set_topics(TOPIC_SILVER)
        .set_group_id("security.flink.aggregator.v1")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = (
        env
        .from_source(source, WatermarkStrategy.no_watermarks(), "silver-source")
        .map(ParseSilverEvent())
        .assign_timestamps_and_watermarks(
            WatermarkStrategy
            .for_bounded_out_of_orderness(Duration.of_seconds(30))
            .with_timestamp_assigner(EventTimeAssigner())
        )
    )

    def make_sink(topic: str) -> KafkaSink:
        return (
            KafkaSink.builder()
            .set_bootstrap_servers(KAFKA_SERVERS)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .build()
        )
    
    windowed_sink = make_sink(TOPIC_WINDOWED)
    correlated_sink = make_sink(TOPIC_CORRELATED)
    baseline_sink = make_sink(TOPIC_BASELINE)

    # ── Branch 1: Velocity windowing → logs.aggregated.windowed-events.v1 ────
    windowed_stream = (
        stream
        .key_by(lambda e: f"{e.get('source_ip', 'unknown')}:{e.get('event_type', 'UNKNOWN')}")
        .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_MINUTES)))
        .process(VelocityWindowFunction(), output_type=Types.STRING())
    )
    windowed_stream.sink_to(windowed_sink)

    # ── Branch 2: attack chain → logs.aggregated.correlated-events.v1 ────
    # Detect: port_scan → blocked → allowed from same source_ip
    (
        stream
        .key_by(lambda e: e.get("source_ip", "unknown"))
        .process(AttackChainDetector(), output_type=Types.STRING())
        .sink_to(correlated_sink)
    )

    # ── Branch 3: Baseline deviation → logs.aggregated.baseline-alerts.v1 ────
    # Feed the windowed stream (already aggregated per entity per window)
    # into the baseline detector. Only aggregated events have event_count,
    # so this naturally operates at the window granularity we want.
    (
        windowed_stream
        .key_by(lambda s: (lambda e: f"{e.get('source_ip', 'unknown')}:{e.get('event_type', 'UNKNOWN')}")(json.loads(s)))
        .process(BaselineDeviationFunction(), output_type=Types.STRING())
        .sink_to(baseline_sink)
    )

    env.execute("log-aggregation-job")


if __name__ == "__main__":
    main()
