# Agentic Network Event Monitor

A real-time security event pipeline that generates synthetic network logs, processes them through a medallion architecture, enriches each event with threat intelligence and an AI-generated assessment, and detects attack patterns using Apache Flink stream processing.

Built with Go, Python, Apache Kafka, Apache Flink, LangGraph, and Ollama — fully containerized with Docker Compose.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Flow](#data-flow)
- [Project Structure](#project-structure)
- [Modules](#modules)
  - [Producer](#producer)
  - [Cleaner Service](#cleaner-service)
  - [Enricher Agent](#enricher-agent)
  - [Flink Stream Processor](#flink-stream-processor)
  - [Gold Consumer](#gold-consumer)
  - [Flink Consumer](#flink-consumer)
- [Storage Layers](#storage-layers)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Future Enhancements](#future-enhancements)

---

## Overview

The pipeline has two halves. The **producer** is a Go application that uses Ollama to generate realistic synthetic security events and streams them into Kafka. The **consumer pipeline** is a set of Python services that clean, enrich, score, and store those events — while Apache Flink runs a parallel analysis stream for detecting patterns invisible at the individual event level.

**Key capabilities:**

- LLM-powered synthetic log generation (Go + Ollama) with disk overflow protection and graceful shutdown
- Normalises and validates raw logs from multiple simulated source types (firewall, EDR, SIEM)
- Enriches each event with IP reputation (AbuseIPDB, OTX, VirusTotal), asset context, and identity risk
- Scores events across eight signal dimensions into a composite 0–100 threat score
- Uses a local LLM (Ollama / llama3.1) to generate analyst narratives, MITRE ATT&CK tactic labels, and recommended actions via a LangGraph agent
- Detects high-velocity attacks, correlated multi-stage attack chains, and slow-burn baseline anomalies via Flink
- Archives all pipeline stages to MinIO, with hot storage in Elasticsearch and warm storage in PostgreSQL

---

## Architecture
[HTML diagram](https://htmlpreview.github.io/?https://github.com/vijay-ss/agentic-network-event-monitor/blob/main/architecture-diagram.html)

```
┌─────────────────────────────────────────────────────────────────-┐
│  PRODUCER                                                        │
│  Go + Ollama (llama3.1)                                          │
│  Synthetic log generation with disk overflow + graceful shutdown │
└────────────────────────────┬────────────────────────────────────-┘
                             │
                             ▼
              [Kafka: logs.bronze.events.v1]
                             │
                             ▼
                    ┌─────────────────┐
                    │ Cleaner Service │  Normalise · Validate · Deduplicate
                    └────────┬────────┘
                             │
                             ▼
              [Kafka: logs.silver.events.v1]
                             │
               ┌─────────────┴────────────────┐
               │                              │
               ▼                              ▼
       Enricher Agent                     Flink Job
       (LangGraph pipeline)               (3 parallel branches)
               │                              │
               ▼                              ▼
  [logs.gold.assessed-events.v1]   [logs.aggregated.*.v1]
               │                              │
               ▼                              ▼
       gold-consumer                  flink-consumer
               │                              │
    ┌──────────┴──────────┐       ┌───────────┴───────-───┐
    │  Elasticsearch      │       │  Elasticsearch        │
    │  security-events    │       │  security-aggregations│
    ├─────────────────────┤       ├──────────────────────-┤
    │  PostgreSQL         │       │  PostgreSQL           │
    │  events table       │       │  aggregations table   │
    └─────────────────────┘       └───────────────────────┘
                    │
              MinIO data lake
        bronze/ silver/ gold/ aggregated/
                    │
             Kibana · Superset 
```

---

## Data Flow

| Stage | Topic | Written by | Read by |
|-------|-------|-----------|---------|
| Bronze | `logs.bronze.events.v1` | Producer (Go) | Cleaner Service |
| Silver | `logs.silver.events.v1` | Cleaner Service | Enricher Agent, Flink Job |
| Gold | `logs.gold.assessed-events.v1` | Enricher Agent | Gold Consumer, Flink Consumer (cache) |
| Velocity | `logs.aggregated.windowed-events.v1` | Flink Job | Flink Consumer |
| Attack chain | `logs.aggregated.correlated-events.v1` | Flink Job | Flink Consumer |
| Baseline | `logs.aggregated.baseline-alerts.v1` | Flink Job | Flink Consumer |
| Dead letter | `logs.dead.*.v1` | Cleaner Service / Enricher Agent | Monitoring |

---

## Project Structure

```
agentic-network-event-monitor/
│
├── docker-compose.yml              # Full stack — all services and dependencies
├── .env.example                    # Environment variable template
│
├── producer/                       # Go: synthetic log generation
│   ├── generator/                  # LLM-based event generation + overflow
│   ├── kafka/                      # Kafka producer client
│   ├── ollama/                     # Ollama API client
│   └── README.md                   # Producer deep-dive
│
├── shared/                         # Python: shared library for all consumer services
│   ├── config.py                   # Centralised config (Kafka topics, ES indices, DSNs)
│   ├── models.py                   # Pydantic models: Bronze, Silver, Gold, Aggregation
│   └── kafka_client.py             # Producer/consumer factory functions
│
├── cleaner/                        # Python: Bronze → Silver (deterministic transforms, no LLM)
│   ├── consumer.py
│   └── transforms.py
│
├── enricher/                       # Python: Silver → Gold (LangGraph agent)
│   ├── consumer.py
│   ├── agent/
│   │   ├── graph.py
│   │   └── tools/
│   │       ├── enrichment.py
│   │       ├── scorer.py
│   │       ├── reasoner.py
│   │       └── router.py
│   └── README.md                   # Enricher deep-dive
│
├── flink/                          # Python: Stream processing
│   ├── jobs/
│   │   └── security_aggregation.py
│   └── README.md                   # Flink deep-dive
│
├── gold_consumer/                  # Python: Gold topic → ES + Postgres
│   └── consumer.py
│
├── flink_consumer/                 # Python: Aggregated topics → ES + Postgres
│   └── consumer.py
│
├── scripts/
│   └── demo_producer.py            # Python demo producer for testing without Go
│
└── config/
    ├── elasticsearch/              # Index mappings + init script
    ├── kafka-connect/              # S3 sink connector registration
    └── postgres/                   # Schema, indexes, and analytical views
```

---

## Modules

### Producer

A Go application that uses Ollama to generate contextually realistic synthetic security events and streams them to Kafka at a configurable rate.

**Key features:**

- LLM-powered generation via Ollama — events are contextually realistic, not templated
- Configurable event rate (e.g. 100 logs/hour)
- Disk overflow protection — if the in-memory buffer fills during a Kafka outage, events spill to disk and are replayed automatically
- Graceful shutdown on `Ctrl+C`, `docker stop`, and `kill` — drains buffer, replays overflow, prints final stats

**Overflow flow:**
```
Normal:   Ollama → Buffer → Kafka
Overload: Ollama → Buffer (FULL) → Disk
Recovery: Disk → Buffer → Kafka
```

**Generated event sample:**
```json
{
  "id": "log_001",
  "timestamp": "2026-02-08T09:15:32.142Z",
  "source_ip": "192.168.1.45",
  "destination_ip": "142.250.185.46",
  "destination_domain": "docs.google.com",
  "source_port": 52341,
  "destination_port": 443,
  "protocol": "TCP",
  "application": "HTTPS",
  "action": "ALLOW",
  "bytes_sent": 2048,
  "bytes_received": 8192,
  "packet_count": 47,
  "duration_ms": 1523,
  "event_type": "HTTPS_CONNECTION",
  "severity": "INFO",
  "user": "john.doe",
  "correlation_id": "corr_a8f3d2c1",
  "device_name": "WS-MKTG-JD-042",
  "vlan_id": 100,
  "geo_location": "Toronto, ON, CA",
  "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0",
  "log_metadata": {
    "collector": "splunk-forwarder-03",
    "source_type": "firewall",
    "ingestion_time": "2026-02-08T09:15:33.245Z",
    "pipeline": "network_events"
  },
  "network_details": {
    "network_segment": "CORPORATE_LAN",
    "subnet": "192.168.1.0/24",
    "security_zone": "trusted"
  },
  "connection_state": "ESTABLISHED",
  "tcp_flags": "ACK,PSH",
  "policy_violation": null,
  "asset_tags": ["production", "standard_workstation"],
  "session_details": null,
  "resource_utilization": null,
  "response_code": 200,
  "threat_indicators": [],
  "rule_id": "FW-ALLOW-001",
  "description": "User accessing Google Docs to collaborate on project proposal"
}
```

See [`producer/README.md`](producer/README.md) for full setup, configuration, overflow tuning, and troubleshooting.

---

### Cleaner Service

Consumes Bronze events and produces cleaned, validated Silver events. Contains no LLM or AI logic — all transforms are deterministic.

- Normalises timestamps to UTC ISO 8601
- Validates and standardises IP addresses, ports, and enum fields
- Flattens nested source-system fields into a consistent schema
- Flags non-fatal issues as `cleaning_warnings` on the event
- Routes unparseable events to `logs.dead.parse-errors.v1`

Consumer group: `security.cleaner.v1`

---

### Enricher Agent

Consumes Silver events and runs a four-stage LangGraph pipeline to produce fully assessed Gold events.

| Stage | What it does |
|-------|-------------|
| **Enrich** | Parallel async calls to AbuseIPDB, OTX, IPinfo, VirusTotal, CMDB, and IdP |
| **Score** | Composite 0–100 threat score across 8 signal dimensions |
| **Reason** | Ollama LLM generates analyst narrative, MITRE ATT&CK tactic, and recommended action |
| **Route** | Maps score to action tier: `archive / digest / ticket / page / contain` |

Events scoring below 15 skip the LLM call to conserve compute.

**Threat score components:**

| Component | Max pts | Signal |
|-----------|---------|--------|
| Severity base | 85 | Source system classification |
| Event type boost | 40 | What actually happened |
| Action modifier | 5 | Allowed vs blocked |
| Policy violation | 25 | Compliance breach severity |
| Security zone | 10 | Untrusted origin |
| IP reputation | 30 | AbuseIPDB abuse score |
| Asset criticality | 25 | Target importance + internet exposure |
| Identity risk | ~35 | Offboarded user, privileged account, risk score |

Consumer group: `security.enricher.v1`

See [`enricher/README.md`](enricher/README.md) for the full scoring model, action tier definitions, and LLM prompt details.

---

### Flink Stream Processor

A PyFlink job that runs as a parallel consumer of `logs.silver.events.v1` using its own consumer group — it never affects the Enricher Agent's offsets.

Three analysis branches run simultaneously:

**1. Velocity windowing** → `logs.aggregated.windowed-events.v1`

Tumbling window aggregation per `source_ip:event_type`. Emits if event count within the window exceeds the configured threshold. Output includes unique destination IPs/ports and total bytes — context the per-event enricher cannot see.

**2. Attack chain detection** → `logs.aggregated.correlated-events.v1`

Detects `port_scan → blocked_attempt → successful_connection` from the same source IP within a configurable window. Implemented using a stateful `KeyedProcessFunction` (`AttackChainDetector`) with an in-memory event buffer and TTL-based eviction. Emits a `CORRELATED_ATTACK_CHAIN` event flagged CRITICAL when the full sequence is observed.

> Note: Flink CEP is a Java-only API and is not available in PyFlink. `AttackChainDetector` replicates the same pattern detection logic using keyed state instead.

**3. Baseline deviation alerting** → `logs.aggregated.baseline-alerts.v1`

Maintains a rolling per-entity baseline of event counts. Alerts when the current window count exceeds the rolling average by a configurable multiplier. Catches slow-burn attacks that stay under the velocity threshold but are still anomalous for a given entity.

Consumer group: `security.flink.aggregator.v1`

See [`flink/README.md`](flink/README.md) for tuning parameters and PyFlink implementation notes — including known constraints around serialisation, CEP availability, and JAR versioning.

---

### Gold Consumer

Reads `logs.gold.assessed-events.v1` and writes to Elasticsearch and PostgreSQL. All writes are idempotent — Elasticsearch uses the silver event ID as the document ID, PostgreSQL uses `ON CONFLICT DO NOTHING` on `silver_id`.

Consumer group: `security.gold-consumer.v1`

---

### Flink Consumer

Reads all three aggregated Kafka topics and writes enriched aggregations to storage.

Uses a two-thread architecture:
- **Cache thread** — reads the gold topic continuously into an in-memory cache keyed by `source_ip:event_type`, with a rolling TTL of 3× the CEP window
- **Consumer thread** — reads aggregated events, joins against the cache to build an `enrichment_summary` (copying threat score, narrative, MITRE tactic from matching gold events), then writes to Elasticsearch and PostgreSQL

No additional API calls are made — enrichment context is borrowed from gold events already processed by the Enricher Agent.

Consumer group: `security.flink.aggregator.v1`

---

## Storage Layers

| Layer | Technology | Index / Table | Purpose |
|-------|-----------|--------------|---------|
| Hot | Elasticsearch | `security-events` | SOC dashboards, real-time search |
| Hot | Elasticsearch | `security-aggregations` | Flink pattern detection output |
| Warm | PostgreSQL | `events`, `aggregations` | Historical BI, SQL analysis |
| Cold | MinIO | `bronze/` `silver/` `gold/` `aggregated/` | Data lake archive |

PostgreSQL ships with pre-built analytical views:

| View | Description |
|------|-------------|
| `v_hourly_threat_trends` | Event counts and score trends by hour |
| `v_top_threat_ips` | Per-IP threat summary with MITRE tactics |
| `v_mitre_coverage` | MITRE ATT&CK tactic distribution |
| `v_action_summary` | Daily action tier breakdown |
| `v_aggregation_enriched` | Flink aggregations joined to their highest-scoring gold event |
| `v_ip_full_picture` | All activity for a given IP across both event and aggregation tables |

---

## Getting Started

**Prerequisites:** Docker Desktop, Docker Compose v2, Go 1.21+

**1. Clone and configure**

```bash
git clone https://github.com/vijay-ss/agentic-network-event-monitor.git
cd agentic-network-event-monitor
cp .env.example .env
```

Edit `.env` and add your API keys:

```env
ABUSEIPDB_API_KEY=your_key
OTX_API_KEY=your_key
IPINFO_TOKEN=your_token
VIRUSTOTAL_API_KEY=your_key
OLLAMA_MODEL=llama3.1
```

**2. Start the full stack**

```bash
docker compose up -d
```

Allow 2–3 minutes on first run for Ollama to pull the model.

**3. Send test events**

```bash
# Python demo producer (no Go required)
docker compose --profile demo run demo-producer

# Or start the Go producer
docker compose up producer
```

**4. Access the dashboards**

| Service | URL |
|---------|-----|
| Kibana | http://localhost:5601 |
| Apache Superset | http://localhost:8088 |
| Flink Web UI | http://localhost:8081 |
| MinIO Console | http://localhost:9001 |
| Kafka Connect | http://localhost:8083 |

**5. Monitor the pipeline**

```bash
# Watch enricher output in real time
docker compose logs enricher-agent -f

# Check Elasticsearch event count
curl -s http://localhost:9200/security-events/_count | python3 -m json.tool

# Query Postgres
docker compose exec postgres psql -U security -d security \
  -c "SELECT count(*), round(avg(threat_score)) as avg_score FROM events;"

# Check Kafka consumer lag
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --describe --group security.enricher.v1
```

---

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `OLLAMA_MODEL` | `llama3.1` | LLM model for event generation and assessment |
| `FLINK_WINDOW_MINUTES` | `5` | Tumbling window size for velocity detection |
| `FLINK_CEP_WINDOW_MINUTES` | `10` | Attack chain detection window |
| `FLINK_VELOCITY_THRESHOLD` | `10` | Minimum events to emit a velocity alert |
| `FLINK_BASELINE_MULTIPLIER` | `3.0` | Multiplier above rolling average to trigger baseline alert |
| `FLINK_BASELINE_WINDOW_COUNT` | `12` | Windows kept in rolling baseline |
| `POSTGRES_PASSWORD` | `security` | PostgreSQL password |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO access key |

---

## Future Enhancements

**Alerting and response**
- Integrate PagerDuty or Slack webhooks for `page` and `contain` action tiers
- Implement automated containment actions (firewall rule push, account suspension) via a dedicated response service

**Pipeline resilience**
- Replace in-memory state in Flink functions with managed state (`ValueState`, `ListState`) for checkpoint recovery
- Add a Schema Registry to validate event structure before it reaches the cleaner
- Dead letter consumer for reprocessing failed events after schema corrections

**Enrichment depth**
- Real CMDB and IdP integrations for live asset and identity context (currently stubbed)
- Redis cache for AbuseIPDB and OTX responses to reduce API call volume and latency
- Dedicated LLM narrative for Flink aggregation events — aggregated context (e.g. "285 SSH attempts in 5 minutes") is richer input than individual events

**Observability**
- Prometheus metrics + Grafana dashboard for pipeline throughput, enrichment latency, and LLM response times
- Kafka consumer lag monitoring via Burrow or Confluent Control Center
- Structured logging with correlation IDs across all services for end-to-end trace visibility

**Scalability**
- Increase Kafka partition count and Flink parallelism for higher throughput
- Elasticsearch ILM policies for automated hot-to-warm-to-cold index lifecycle

**Model improvements**
- Benchmark smaller models (Mistral, Phi-3) against llama3.1 for the enrichment task
- Fine-tune on historical analyst verdicts to improve recommended action accuracy
- Add confidence scoring to LLM outputs so low-confidence assessments are flagged for human review

---

## Contributing

Contributions welcome. Areas of interest:

- Additional event types (cloud, container, API gateway)
- Attack scenario templates for the producer
- Multi-model support for the enricher (OpenAI, Anthropic)
- Web UI for pipeline monitoring

---

## License

MIT License — see [LICENSE](LICENSE) for details.

---

Built with [Ollama](https://ollama.ai) · [Apache Kafka](https://kafka.apache.org) · [Apache Flink](https://flink.apache.org) · [LangGraph](https://langchain-ai.github.io/langgraph/) · [Elasticsearch](https://www.elastic.co) · [Sarama](https://github.com/IBM/sarama)
