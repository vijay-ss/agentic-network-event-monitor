# Agentic Network Event Monitor

An intelligent security log generation system powered by Ollama LLMs that produces realistic, diverse network security events for SIEM testing, and security analytics development.

## What It Does

This system generates **production-quality synthetic security logs** that simulate real-world network traffic, security events, and threats. Think of it as a "synthetic data factory" for security operations.

### Key Features
- 🤖 **LLM-Powered Generation** - Uses Ollama to create contextually realistic security events
- 📊 **Kafka Integration** - Streams events in real-time to Kafka topics
- 💾 **Disk Overflow Protection** - Never loses logs, even during Kafka outages
- 🔄 **Graceful Shutdown** - Handles Docker stop, Ctrl+C, and kill signals cleanly
- 🎭 **Diverse Event Types** - Network traffic, threats, authentication, policy violations
- ⚡ **Production-Ready** - Includes monitoring, metrics, and error recovery

## Architecture
```
┌─────────────┐
│   Ollama    │ Generates realistic security events
│  (LLM API)  │ using prompt engineering
└──────┬──────┘
       │
       ▼
┌─────────────────────┐
│  Stream Generator   │ Produces events at configurable rate
│  - Disk overflow    │ (e.g., 100 logs/hour)
│  - Graceful shutdown│
└──────┬──────────────┘
       │
       ▼
┌─────────────┐       ┌──────────────┐
│   Buffer    │──────▶│    Kafka     │ Real-time streaming
│  (in-memory)│       │ (security-logs)│
└─────┬───────┘       └──────────────┘
      │
      ▼ (if full)
┌─────────────┐
│ Disk Backup │ Overflow protection
│ (JSON Lines)│
└─────────────┘
```

# Contents
- [Architecture](#architecture)
- [Key Features](#key-features)
- [Quick Start](#quick-start)
- [Producer App](#producer-app)
  * [Basic Streaming Setup](#basic-streaming-setup)
  * [Features in Detail](#features-in-detail)
    * [Backpressure Handling](#backpressure-handling)
    * [Graceful Shutdown](#graceful-shutdown)
    * [Generated Log Format](#generated-log-format)
  * [Monitoring](#monitoring)
  * [Troubleshooting](#troubleshooting)
- [Consumer App App](#consumer-app)

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Go 1.21+
- [Ollama](https://ollama.ai) installed locally

### Build and run Docker Compose

```bash
docker compose build

docker compose up
```
# Producer App

The producer app lives in a docker container which executes a Go module calling upon a local Ollama instance to generate the security logs. These logs are parsed and passed to Kafka for downstream consumption.

## Basic Streaming Setup

```go
import (
    "go-log-producer/generator"
    "go-log-producer/kafka"
    "go-log-producer/ollama"
)

func main() {
    // Setup Ollama client
    client := ollama.NewClient("http://localhost:11434")
    
    // Setup Kafka producer
    producer, _ := kafka.NewKafkaProducer(
        []string{"localhost:9092"}, 
        "security-logs",
    )
    
    // Create LLM request
    request := &ollama.ChatRequest{
        Model: "llama3.1",
        Messages: []ollama.Message{
            {Role: "system", Content: "Generate realistic security logs..."},
            {Role: "user", Content: "Generate 10 security events"},
        },
    }
    
    // Start streaming with disk overflow protection
    generator.StreamLogsWithDiskOverflow(
        client,
        request,
        producer,
        100,                  // Target: 100 logs/hour
        500,                  // Buffer: 500 logs
        "overflow_logs.jsonl", // Overflow file
    )
}
```

### Configuration

Adjust generation rate and buffer size:

```go
generator.StreamLogsWithDiskOverflow(
    client,
    request,
    producer,
    1000,  // Generate 1000 logs/hour (1 every 3.6 seconds)
    1000,  // Buffer holds 1000 logs before overflow
    "overflow_logs.jsonl",
)
```

## Features in Detail

### Backpressure Handling

When the in-memory buffer fills up (e.g., Kafka is slow), logs are automatically saved to disk and replayed later.

Backpressure prevents system overload when log generation is faster than Kafka can consume. Instead of crashing with out-of-memory errors, the system gracefully drops excess logs when the buffer fills up.

```
Normal: Ollama → Buffer → Kafka ✅
Overload: Ollama → Buffer (FULL) → Disk 💾
Recovery: Disk → Buffer → Kafka ♻️
```

```
Ollama (generates logs) → [Buffer Queue] → Kafka (consumes logs)
                               ↓ (if full)
                          Dropped logs (logged as warnings)
```

### How It Works

1. **Buffered Queue**: Holds logs in memory (configurable size, e.g., 500 logs)
2. **Producer**: Generates logs at target rate (e.g., 3600 logs/hour)
3. **Consumer**: Sends logs to Kafka at its own pace
4. **Safety Valve**: If buffer fills up, excess logs are dropped (not crash)

**Benefits:**
- ✅ **100% log retention** - Never lose logs, even during outages
- ✅ **Non-blocking** - Producer never slows down
- ✅ **Automatic replay** - Background process replays overflow logs every minute
- ✅ **Crash recovery** - Overflow file persists across restarts

**Example output:**
```
Stats: Generated=1000, InBuffer=450, OnDisk=50, SentToKafka=950
📁 Buffer full (500/500), saved log to disk: log_1001
📁 Replaying overflow file (12560 bytes)...
✅ Replayed 50 logs from disk
```

More details on backpressure: [Backpressure Guide](producer/generator/Readme.md)

### Graceful Shutdown

Handles **Ctrl+C**, **docker stop**, and **kill** signals cleanly.

```bash
# Press Ctrl+C or run: docker stop <container>
```

**Shutdown sequence:**
```
⚠️  Received signal: interrupt - shutting down gracefully...
Producer: shutdown signal received, stopping...
Producer: stopped
Monitor: final overflow replay before shutdown...
Monitor: stopped
Consumer: draining 3 remaining logs from buffer...
Consumer: buffer drained
Consumer: stopped
✅ All goroutines stopped cleanly

=== FINAL STATS ===
Generated:     1450 logs
Sent to Kafka: 1450 logs
On disk:       0 logs
👋 Shutdown complete
```

**Features:**
- Waits up to 30 seconds for clean shutdown
- Drains in-memory buffer to Kafka
- Attempts final overflow replay
- Prints final statistics

### Generated Log Format

The system generates logs with rich, realistic fields:
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

#### Event Types Generated

- 🌐 **Network Traffic**: HTTP/HTTPS, DNS, SSH, RDP, VPN
- 🚨 **Threats**: Malware, ransomware, port scans, brute force
- 🔐 **Authentication**: Login success/failure, MFA events
- 🛡️ **Policy Violations**: Blocked connections, policy denials
- 📁 **File Operations**: Downloads, uploads, suspicious files
- 🔄 **System Events**: Service starts/stops, configuration changes

### Check Overflow File

```bash
# View overflow logs (JSON Lines format)
cat overflow_logs.jsonl | jq .

# Count overflow logs
wc -l overflow_logs.jsonl
```

## Monitoring

The system logs comprehensive statistics:

```
Stats: Generated=1000, InBuffer=450, OnDisk=50, SentToKafka=950
```

| Metric | Description |
|--------|-------------|
| **Generated** | Total logs created by Ollama |
| **InBuffer** | Logs currently in memory buffer |
| **OnDisk** | Logs saved to overflow file |
| **SentToKafka** | Logs successfully sent to Kafka |

### Alerts to Watch For

⚠️ **High disk usage**: If `OnDisk` stays high, Kafka may be falling behind
⚠️ **Parse errors**: If you see frequent JSON parsing errors, check model output
⚠️ **Ollama timeouts**: Consider increasing timeout or using faster model

## Troubleshooting

### Ollama Connection Issues

```bash
# Check Ollama is running
curl http://localhost:11434/api/tags

# Test model
ollama run llama3.1 "Say hello"
```

### Kafka Connection Issues

```bash
# Check Kafka is running
docker compose ps

# List topics
docker compose exec kafka kafka-topics.sh \
  --list --bootstrap-server localhost:9092
```

### Parsing Errors

If you see JSON parsing errors:

1. Check Ollama model output quality
2. Try a larger model (qwen2.5:32b recommended)
3. Review prompt engineering in the code

# Consumer App
Coming soon...

## 🤝 Contributing

Contributions welcome! Areas for improvement:

- [ ] Additional event types (cloud, container, API)
- [ ] Configurable log schemas
- [ ] Prometheus metrics integration
- [ ] Multi-model support (OpenAI, Anthropic)
- [ ] Web UI for monitoring
- [ ] Attack scenario templates

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Acknowledgments

Built with:
- [Ollama](https://ollama.ai) - Local LLM inference
- [Sarama](https://github.com/IBM/sarama) - Kafka client for Go
- [Docker](https://docker.com) - Containerization

## 📚 Documentation

- [Ollama API Documentation](https://github.com/ollama/ollama/blob/main/docs/api.md)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Architecture Deep Dive](docs/ARCHITECTURE.md) *(coming soon)*

## 💡 Use Cases

- **SIEM Testing**: Generate realistic data for testing detection rules
- **ML Training**: Create large datasets for security ML models
- **Demo Environments**: Populate security dashboards with live-looking data
- **Load Testing**: Test Kafka/SIEM infrastructure with realistic traffic
- **Security Research**: Generate attack scenarios for analysis

---

**Need help?** Open an issue or reach out on [GitHub Discussions](https://github.com/vijay-ss/agentic-network-event-monitor/discussions)

**Star this repo** ⭐ if you find it useful!
