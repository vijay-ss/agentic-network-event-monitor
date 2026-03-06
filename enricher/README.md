# Enricher Agent

Consumes cleaned Silver events from Kafka, runs them through a four-stage LangGraph pipeline, and produces fully assessed Gold events with a composite threat score, MITRE ATT&CK tactic, analyst narrative, and recommended action tier.

---

## Table of Contents

- [Overview](#overview)
- [Pipeline Stages](#pipeline-stages)
- [Threat Scoring](#threat-scoring)
- [Action Tiers](#action-tiers)
- [LLM Reasoning](#llm-reasoning)
- [Configuration](#configuration)

---

## Overview

```
logs.silver.events.v1
        │
        ▼
   ┌─────────┐
   │  Enrich │  Async threat intel API calls
   └────┬────┘
        │
        ▼
   ┌─────────┐
   │  Score  │  Composite 0–100 threat score
   └────┬────┘
        │
        ▼
   ┌─────────┐      (skipped if score < 15)
   │  Reason │  Ollama LLM — narrative + MITRE + action
   └────┬────┘
        │
        ▼
   ┌─────────┐
   │  Route  │  Emit to gold topic + console output
   └────┬────┘
        │
        ▼
logs.gold.assessed-events.v1
```

Consumer group: `security.enricher.v1`

---

## Pipeline Stages

### 1. Enrich

Runs all external lookups concurrently using `asyncio.gather`. Each lookup is independent — a timeout or API error on one does not block the others.

| Source | Data retrieved |
|--------|---------------|
| AbuseIPDB | `abuse_score`, `is_malicious`, report count |
| AlienVault OTX | Pulse count, malware families, threat types |
| IPinfo | ASN, org, country, hosting provider flag |
| VirusTotal | Malicious engine count, threat labels |
| CMDB (stub) | Asset criticality (0–10), internet-facing flag, owner |
| IdP (stub) | `is_privileged`, `recent_offboard`, rolling risk score |

Results are merged into `AgentState` before scoring. If all external calls fail, scoring proceeds with zero enrichment scores — the event is never dropped.

### 2. Score

Produces a composite threat score from 0 to 100 across eight signal dimensions. See [Threat Scoring](#threat-scoring) for the full breakdown.

### 3. Reason

Calls Ollama via `langchain-ollama` with a structured system prompt. The LLM receives the full enriched event as JSON and returns a structured assessment.

Events with a composite score below 15 skip this stage entirely — low-signal informational events do not warrant LLM compute.

Output fields:
- `recommended_action` — one of: `archive`, `digest`, `ticket`, `page`, `contain`
- `mitre_tactic` — MITRE ATT&CK tactic name, or null
- `narrative` — 2–3 sentence plain-English analyst summary

### 4. Route

Maps the composite score to an action tier, emits the Gold event to `logs.gold.assessed-events.v1`, and prints a Rich summary panel to stdout.

---

## Threat Scoring

Each signal dimension contributes independently. The final composite is capped at 100.

| Dimension | Max pts | Description |
|-----------|---------|-------------|
| Severity base | 85 | Classification from the source system (INFO=5, LOW=15, MEDIUM=35, HIGH=60, CRITICAL=85) |
| Event type boost | 40 | What actually happened, regardless of severity label. `MALWARE_DETECTED`=40, `DATA_EXFILTRATION`=35, `PRIVILEGE_ESCALATION`=30 |
| Action modifier | 5 | +5 if the event was allowed through rather than blocked — allowed suspicious traffic represents actual exposure |
| Policy violation | 25 | Scales with violation severity: low=5, medium=15, high=25 |
| Security zone | 10 | +10 if the event originates from the `untrusted` zone |
| IP reputation | 30 | `abuse_score // 2`, capped at 30 — a maximally malicious IP cannot dominate the score alone |
| Asset criticality | 25 | `(criticality / 10) * 20` + 5 if internet-facing |
| Identity risk | ~35 | +25 recent offboard, +10 privileged account, + `risk_score // 10` |

**Note:** High-severity events can reach 100 from severity base + event type boost alone (e.g. CRITICAL + MALWARE_DETECTED = 125, capped at 100). In these cases the enrichment signals are still collected but have no visible effect on the final score — they remain available in the breakdown for analyst review.

See [`scorer.py`](agent/tools/scorer.py) and [`scorer-documentation.md`](../docs/scorer-documentation.md) for the full implementation.

---

## Action Tiers

| Score range | Action | Meaning |
|-------------|--------|---------|
| 0–19 | `archive` | Noise / informational. Log and retain, no action. |
| 20–39 | `digest` | Low signal. Include in daily digest report. |
| 40–59 | `ticket` | Medium risk. Open analyst queue ticket. |
| 60–79 | `page` | High risk. Page on-call immediately. |
| 80–100 | `contain` | Critical. Immediate automated or manual containment + P1 incident. |

---

## LLM Reasoning

The reasoner sends the full enriched event context to Ollama and parses the response as structured JSON.

**System prompt (abridged):**
```
You are an expert security analyst. You receive enriched network security log
data and produce a structured assessment.

Respond with valid JSON only — no markdown, no explanation outside the JSON.

Response schema:
{
  "recommended_action": "<archive | digest | ticket | page | contain>",
  "mitre_tactic": "<MITRE ATT&CK tactic name or null>",
  "narrative": "<2-3 sentence plain-English analyst summary>"
}
```

The parser strips markdown fences and falls back to regex extraction if the model returns malformed JSON. If parsing fails entirely, the event is still emitted with `recommended_action: ticket` and a default narrative rather than being dropped.

**Implementation notes:**
- Uses `ChatOllama` from `langchain-ollama` with `temperature=0.1` for deterministic output
- `response.content` must be extracted before parsing — `llm.invoke()` returns an `AIMessage` object, not a string
- Per-call timeout of 30 seconds — OTX and other slow APIs share a session but each call gets its own budget

---

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `OLLAMA_BASE_URL` | `http://ollama:11434` | Ollama server URL |
| `OLLAMA_MODEL` | `llama3.1` | Model to use for assessment |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka broker address |
| `ABUSEIPDB_API_KEY` | — | Required for IP reputation |
| `OTX_API_KEY` | — | Required for OTX threat intel |
| `IPINFO_TOKEN` | — | Required for ASN/org lookup |
| `VIRUSTOTAL_API_KEY` | — | Required for VirusTotal scan |

All values are read from environment variables via [`shared/config.py`](../shared/config.py).
