# Producer

A Go application that uses Ollama to generate contextually realistic synthetic security events and streams them to Kafka at a configurable rate. Includes disk overflow protection and graceful shutdown handling.

---

## Table of Contents

- [Overview](#overview)
- [Backpressure and Overflow](#backpressure-and-overflow)
- [Graceful Shutdown](#graceful-shutdown)
- [Configuration](#configuration)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

---

## Overview

```
Ollama (llama3.1)
      │
      ▼ generates batches of 25 events
 In-memory buffer
      │
      ├── (buffer has space) ──► Kafka: logs.bronze.events.v1
      │
      └── (buffer full) ──► Disk: overflow_logs.jsonl
                                      │
                               Background replayer
                                      │
                                      ▼
                               Kafka (when space available)
```

Events are generated in batches using a structured Ollama prompt and parsed from the LLM JSON response. Each event is a realistic network security log — see the root README for the full event schema.

---

## Backpressure and Overflow

The producer runs at a configurable rate (e.g. 100 logs/hour). Kafka consumption may be slower than generation during backpressure events — connection issues, broker restarts, or topic lag. Rather than crashing or blocking the generator, excess logs spill to disk.

**Flow states:**

```
Normal:   Ollama → Buffer → Kafka
Overload: Ollama → Buffer (FULL) → Disk
Recovery: Disk → Buffer → Kafka
```

**Buffer behaviour:**

The in-memory buffer is a Go channel with a configurable capacity. When the buffer is full, new logs are written to `overflow_logs.jsonl` rather than dropped or blocked. A background goroutine replays the overflow file every 60 seconds, draining it back through the buffer as capacity allows.

```
Buffer at capacity:
┌───┬───┬───┬───┬───┬───┬───┬───┐
│Log│Log│Log│Log│Log│Log│Log│Log│  ← Full
└───┴───┴───┴───┴───┴───┴───┴───┘
         ↓ excess written to
  overflow_logs.jsonl (JSON Lines)
```

**Timing:**

The generator calculates the batch interval from `targetLogsPerHour` and a fixed batch size of 25:

```
targetLogsPerHour = 3600
batchSize = 25
interval = 1 hour / (3600 / 25) = 25 seconds

→ Every 25 seconds: call Ollama, parse 25 events, push to buffer
```

**Configuration:**

```go
generator.StreamLogsWithDiskOverflow(
    client,
    request,
    producer,
    100,                    // targetLogsPerHour
    500,                    // maxBufferSize
    "overflow_logs.jsonl",  // overflow file path
)
```

| Parameter | Description |
|-----------|-------------|
| `targetLogsPerHour` | Desired generation rate |
| `maxBufferSize` | In-memory buffer capacity before spilling to disk |
| overflow file | Path for overflow JSON Lines file — persists across restarts |

**Tuning `maxBufferSize`:**

| Size | Trade-off |
|------|-----------|
| < 100 | Low memory use, more frequent disk spills |
| 100–1000 | Recommended range for most deployments |
| > 1000 | Absorbs larger bursts, higher memory footprint |

---

## Graceful Shutdown

Handles `SIGINT` (Ctrl+C), `SIGTERM` (docker stop), and `SIGKILL` gracefully.

**Shutdown sequence:**

```
Signal received
      │
      ▼
Producer stops generating new batches
      │
      ▼
Buffer drains remaining logs to Kafka (up to 30s)
      │
      ▼
Background replayer attempts final overflow replay
      │
      ▼
Final stats printed — all goroutines exit cleanly
```

**Example output:**

```
Received signal: interrupt — shutting down gracefully...
Producer: stopped
Monitor: final overflow replay before shutdown...
Consumer: draining 3 remaining logs from buffer...
Consumer: stopped

=== FINAL STATS ===
Generated:     1450 logs
Sent to Kafka: 1450 logs
On disk:       0 logs

Shutdown complete
```

---

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama server URL |
| `OLLAMA_MODEL` | `llama3.1` | Model used for log generation |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `logs.bronze.events.v1` | Target topic |
| `TARGET_LOGS_PER_HOUR` | `100` | Generation rate |
| `MAX_BUFFER_SIZE` | `500` | In-memory buffer capacity |

---

## Monitoring

Stats are logged every 10 seconds:

```
Stats: Generated=1000, InBuffer=450, OnDisk=50, SentToKafka=950
```

| Metric | Description |
|--------|-------------|
| `Generated` | Total events created by Ollama |
| `InBuffer` | Events currently in the in-memory buffer |
| `OnDisk` | Events written to the overflow file |
| `SentToKafka` | Events successfully delivered to Kafka |

**Alerts to watch for:**

- `OnDisk` growing steadily — Kafka may be falling behind; consider increasing `maxBufferSize` or reducing generation rate
- Frequent JSON parse errors — check model output quality; larger models produce more consistent JSON
- Ollama timeouts — increase per-request timeout or switch to a faster model

---

## Troubleshooting

**Ollama connection issues**

```bash
# Verify Ollama is running and the model is available
curl http://localhost:11434/api/tags
ollama run llama3.1 "Generate 1 security log as JSON"
```

**Kafka connection issues**

```bash
# Check Kafka is healthy
docker compose ps kafka

# List topics
docker compose exec kafka kafka-topics \
  --list --bootstrap-server localhost:9092
```

**Overflow file not draining**

```bash
# Check current overflow log count
wc -l overflow_logs.jsonl

# Inspect contents
cat overflow_logs.jsonl | head -5 | python3 -m json.tool
```

If the overflow file continues growing, Kafka is likely not keeping up with the configured rate. Reduce `TARGET_LOGS_PER_HOUR` or check consumer group lag on the bronze topic:

```bash
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group security.cleaner.v1
```

For further detail on the backpressure implementation see [`generator/`](generator/).
