# agentic-network-event-monitor
Monitors real-time NDR/SIEM events with agentic ai capabilities.


## Folder structure
```
project-root/
│
├── docker-compose.yml
├── README.md
│
├── producer/                  # Go Kafka producer
│   ├── Dockerfile
│   ├── go.mod
│   ├── go.sum
│   ├── main.go
│   ├── config/
│   │   └── config.go           # Env + Kafka config
│   └── internal/
│       └── kafka/
│           └── producer.go
│
├── consumer/                  # Python Kafka consumer
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── consumer.py
│   ├── db/
│   │   ├── __init__.py
│   │   ├── connection.py       # Postgres connection
│   │   └── repository.py       # DB inserts
│   └── processing/
│       ├── __init__.py
│       └── cleaner.py          # Data cleaning logic
│
├── db/
│   └── migrations/
│       └── 001_create_table.sql
│
├── scripts/
│   ├── create-topics.sh
│   └── wait-for-kafka.sh
│
└── .env                        # Shared environment variables
```

## Backpressure Handling

The `streamLogsWithBackpressure` function provides a production-ready pattern for continuous log generation with built-in flow control.

### What is Backpressure?

Backpressure prevents system overload when log generation is faster than Kafka can consume. Instead of crashing with out-of-memory errors, the system gracefully drops excess logs when the buffer fills up.

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

### Usage

```go
// Continuous generation with backpressure
err := streamLogsWithBackpressure(
    llmClient, 
    kafkaProducer,
    3600,  // Target: 3600 logs/hour (1 per second)
    500,   // Buffer size: 500 logs
)
```

### When to Use

- ✅ **Use for**: 24/7 continuous generation, system demosetc.
- ❌ **Don't use for**: Fixed batch generation (use `GenerateLogsInBatches` instead)

### Trade-offs

- **Benefit**: System stays stable, no memory crashes
- **Cost**: Some logs may be dropped during overload (logged as warnings)

For most use cases, prefer sequential batching (`GenerateLogsInBatches`) which guarantees all logs are generated and sent.