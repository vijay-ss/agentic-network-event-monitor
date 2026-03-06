# Flink Stream Processor

A PyFlink job that runs three parallel analysis branches on the Silver event stream. Operates as an independent consumer — it never affects the Enricher Agent's Kafka offsets.

---

## Table of Contents

- [Overview](#overview)
- [Analysis Branches](#analysis-branches)
- [Configuration](#configuration)
- [PyFlink Implementation Notes](#pyflink-implementation-notes)

---

## Overview

```
logs.silver.events.v1
  (group: security.flink.aggregator.v1)
        │
        ├──────────────────────┬──────────────────────┐
        │                      │                      │
        ▼                      ▼                      ▼
  Velocity windowing    Attack chain          Baseline deviation
  (tumbling window)     (AttackChainDetector) (BaselineDeviationFunction)
        │                      │                      │
        ▼                      ▼                      ▼
logs.aggregated.        logs.aggregated.       logs.aggregated.
windowed-events.v1      correlated-events.v1   baseline-alerts.v1
```

All three branches consume the same Silver stream simultaneously. Each emits to its own Kafka topic, which is consumed by the `flink-consumer` service for storage to Elasticsearch and PostgreSQL.

---

## Analysis Branches

### 1. Velocity Windowing

**Output:** `logs.aggregated.windowed-events.v1`

Groups events by `source_ip:event_type` using a tumbling event-time window. At the end of each window, if the event count meets or exceeds `FLINK_VELOCITY_THRESHOLD`, a single aggregated event is emitted summarising the window.

The aggregated event includes:
- `event_count` — total events in the window
- `unique_dest_ips` — number of distinct destination IPs targeted
- `unique_dest_ports` — number of distinct destination ports targeted
- `total_bytes_sent` — sum of bytes across all events in the window
- `first_seen` / `last_seen` — window time boundaries

This provides velocity context the per-event Enricher Agent cannot see — 500 individual `FAILED_LOGIN` events each score independently, but the aggregation reveals the brute force pattern.

**Key class:** `VelocityWindowFunction(ProcessWindowFunction)`

### 2. Attack Chain Detection

**Output:** `logs.aggregated.correlated-events.v1`

Detects the sequence `port_scan → blocked_attempt → successful_connection` from the same source IP within a configurable window. Emits a `CORRELATED_ATTACK_CHAIN` event flagged `CRITICAL` when the full sequence is observed, including references to each constituent event.

Implemented as a stateful `KeyedProcessFunction` with a per-key in-memory event buffer. On each new event the buffer is evicted of entries older than the window, then the new event is classified and stored. When a `ALLOW` action is seen and the buffer already contains both a port scan and a blocked attempt, the chain is complete and an alert is emitted.

> **Note:** Flink CEP is a Java-only API — `pyflink.cep` does not exist. `AttackChainDetector` replicates the same pattern detection logic using keyed state rather than the CEP engine.

**Key class:** `AttackChainDetector(KeyedProcessFunction)`

### 3. Baseline Deviation Alerting

**Output:** `logs.aggregated.baseline-alerts.v1`

Feeds off the velocity windowing output. For each aggregated window event, maintains a rolling history of `event_count` values per `source_ip:event_type` key. When the current window count exceeds the rolling average by `FLINK_BASELINE_MULTIPLIER`, an alert is emitted.

This catches slow-burn attacks that stay under the velocity threshold but are still statistically anomalous for a given entity. A device that normally generates 5 failed logins per window will trigger at 15+ (3× multiplier), even if the absolute velocity threshold is set to 50.

The alert includes `baseline_stats` with the current count, rolling average, observed multiplier, and number of windows in the baseline — giving analysts the full context for the deviation.

**Key class:** `BaselineDeviationFunction(KeyedProcessFunction)`

---

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `FLINK_WINDOW_MINUTES` | `5` | Tumbling window size for velocity detection |
| `FLINK_CEP_WINDOW_MINUTES` | `10` | Attack chain detection window — how long a port scan stays in the buffer before being evicted |
| `FLINK_VELOCITY_THRESHOLD` | `10` | Minimum events per window to emit a velocity alert |
| `FLINK_BASELINE_MULTIPLIER` | `3.0` | Multiplier above rolling average to trigger a baseline alert |
| `FLINK_BASELINE_WINDOW_COUNT` | `12` | Number of past windows kept in the rolling baseline |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka broker address |

All values are read from environment variables via [`shared/config.py`](../shared/config.py).

---

## PyFlink Implementation Notes

Several non-obvious constraints apply when writing PyFlink jobs. These are documented here because they caused real failures and are not well-covered in official documentation.

**Never pass Python dicts between operators.**
PyFlink serialises Python objects to Java byte arrays between operators. If a downstream operator or sink expects a string, it will receive `[B` (byte array) instead and throw a `ClassCastException`. Always `yield json.dumps(...)` at the point of emission rather than yielding a dict.

**Always declare `output_type=Types.STRING()` on operators feeding a Kafka sink.**
Without an explicit type annotation, PyFlink cannot infer that the output is a Java `String` and defaults to pickle serialisation, which produces the byte array problem above. Every `.process()` and `.map()` call in the chain leading to a `KafkaSink` using `SimpleStringSchema` must carry `output_type=Types.STRING()`.

**Never use `Types.MAP(Types.STRING(), Types.STRING())` for dicts with numeric values.**
PyFlink's `MapCoder` enforces the declared value type strictly. If the dict contains integers or floats, the coder will fail trying to call `.encode()` on a non-string. Use `json.dumps()` and pass strings instead.

**`ProcessWindowFunction` must `yield`, not `return []`.**
Returning a list wraps the entire list as a single element. PyFlink treats the function as a generator — yield individual elements one at a time.

**`pyflink.cep` does not exist.**
The Flink CEP library is Java-only. Use `KeyedProcessFunction` with a keyed state buffer to replicate pattern detection in Python.

**The Flink Kafka connector JAR must be downloaded separately.**
The `flink-sql-connector-kafka` fat JAR is not bundled in the `flink` base Docker image. It must be downloaded at build time and placed in `/opt/flink/lib/`. The JAR version must match the `flink-dist` version exactly — the image tag and the actual runtime version may differ. Check with:

```bash
docker run --rm flink:1.19-scala_2.12-java11 flink --version
```

Then download the matching connector:

```dockerfile
RUN wget -q -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar
```

**State backend must be `hashmap`, not `filesystem`.**
The `filesystem` state backend alias was removed in Flink 1.13. Use `state.backend: hashmap` in `FLINK_PROPERTIES` for development. For production with large state, use `rocksdb` (requires an additional JAR).
