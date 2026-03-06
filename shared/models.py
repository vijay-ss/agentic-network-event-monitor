# shared/models.py
"""
Shared Pydantic models used by both cleaner and enricher agents.
Schema matches the actual log format from your source system.
"""
from __future__ import annotations
from typing import Optional, Any
from pydantic import BaseModel, Field
from enum import Enum


# ── Enums ─────────────────────────────────────────────────────────────────────

class Severity(str, Enum):
    INFO     = "INFO"
    LOW      = "LOW"
    MEDIUM   = "MEDIUM"
    HIGH     = "HIGH"
    CRITICAL = "CRITICAL"


class Action(str, Enum):
    ALLOW = "ALLOW"
    BLOCK = "BLOCK"
    ALERT = "ALERT"
    DROP  = "DROP"


class ActionTier(str, Enum):
    ARCHIVE = "archive"
    DIGEST  = "digest"
    TICKET  = "ticket"
    PAGE    = "page"
    CONTAIN = "contain"


class ProcessingStatus(str, Enum):
    OK           = "ok"
    PARSE_ERROR  = "parse_error"
    CLEAN_ERROR  = "clean_error"
    ENRICH_ERROR = "enrich_error"


# ── Raw log sub-models ────────────────────────────────────────────────────────

class LogMetadata(BaseModel):
    collector:      Optional[str] = None
    source_type:    Optional[str] = None
    ingestion_time: Optional[str] = None
    pipeline:       Optional[str] = None


class NetworkDetails(BaseModel):
    network_segment: Optional[str] = None
    subnet:          Optional[str] = None
    security_zone:   Optional[str] = None


class PolicyViolation(BaseModel):
    policy_name:      Optional[str] = None
    violation_type:   Optional[str] = None
    policy_severity:  Optional[str] = None


class ResourceUtilization(BaseModel):
    cpu_percent:    Optional[float] = None
    memory_percent: Optional[float] = None
    disk_io_percent: Optional[float] = None
    process_name:   Optional[str]  = None
    process_id:     Optional[int]  = None


# ── Bronze: raw log entry off Kafka ──────────────────────────────────────────

class BronzeEvent(BaseModel):
    """
    Raw log exactly as received from the source system.
    No transformation — faithfully represents the Kafka message.
    """
    id:                   str
    timestamp:            str
    source_ip:            Optional[str]   = None
    destination_ip:       Optional[str]   = None
    destination_domain:   Optional[str]   = None
    source_port:          Optional[int]   = None
    destination_port:     Optional[int]   = None
    protocol:             Optional[str]   = None
    application:          Optional[str]   = None
    action:               Optional[str]   = None
    bytes_sent:           Optional[int]   = None
    bytes_received:       Optional[int]   = None
    packet_count:         Optional[int]   = None
    duration_ms:          Optional[int]   = None
    event_type:           Optional[str]   = None
    severity:             Optional[str]   = None
    user:                 Optional[str]   = None
    correlation_id:       Optional[str]   = None
    device_name:          Optional[str]   = None
    vlan_id:              Optional[int]   = None
    geo_location:         Optional[str]   = None
    user_agent:           Optional[str]   = None
    log_metadata:         Optional[LogMetadata]          = None
    network_details:      Optional[NetworkDetails]       = None
    connection_state:     Optional[str]   = None
    tcp_flags:            Optional[str]   = None
    policy_violation:     Optional[PolicyViolation]      = None
    asset_tags:           list[str]       = []
    session_details:      Optional[Any]   = None
    resource_utilization: Optional[ResourceUtilization]  = None
    response_code:        Optional[int]   = None
    threat_indicators:    list[Any]       = []
    rule_id:              Optional[str]   = None
    reason:               Optional[str]   = None
    description:          Optional[str]   = None
    raw:                  dict[str, Any]  = {}


# ── Silver: cleaned, normalized event ────────────────────────────────────────

class SilverEvent(BaseModel):
    """
    Cleaned and normalized event. Consistent field names, validated
    types, standardized timestamps and IPs, nulls handled explicitly.
    """
    id:                str
    original_id:       str

    # Timestamps UTC ISO 8601
    event_time:        str
    ingestion_time:    Optional[str] = None

    # Network
    source_ip:         Optional[str] = None
    destination_ip:    Optional[str] = None
    destination_domain: Optional[str] = None
    source_port:       Optional[int] = None
    destination_port:  Optional[int] = None
    protocol:          Optional[str] = None
    application:       Optional[str] = None
    bytes_sent:        int = 0
    bytes_received:    int = 0
    packet_count:      int = 0
    duration_ms:       int = 0

    # Event classification
    event_type:        str
    severity:          Severity
    action:            Optional[Action] = None
    connection_state:  Optional[str]   = None
    tcp_flags:         list[str]       = []
    response_code:     Optional[int]   = None

    # Identity
    user:              Optional[str]   = None
    device_name:       Optional[str]   = None

    # Context
    source_type:       Optional[str]   = None   # from log_metadata
    pipeline:          Optional[str]   = None   # from log_metadata
    collector:         Optional[str]   = None   # from log_metadata
    network_segment:   Optional[str]   = None
    security_zone:     Optional[str]   = None
    geo_location:      Optional[str]   = None
    vlan_id:           Optional[int]   = None
    asset_tags:        list[str]       = []
    rule_id:           Optional[str]   = None
    correlation_id:    Optional[str]   = None

    # Policy
    policy_violation:  Optional[PolicyViolation]     = None
    threat_indicators: list[Any]       = []

    # Endpoint (EDR)
    process_name:      Optional[str]   = None
    process_id:        Optional[int]   = None
    cpu_percent:       Optional[float] = None
    memory_percent:    Optional[float] = None

    # Cleaning metadata
    cleaning_version:  str = "1.0"
    cleaning_warnings: list[str] = []


# ── Enrichment sub-models ─────────────────────────────────────────────────────

class IPReputation(BaseModel):
    ip:           str
    is_malicious: bool    = False
    threat_types: list[str] = []
    country:      Optional[str] = None
    abuse_score:  int     = 0
    source:       str     = "unknown"


class AssetContext(BaseModel):
    host:            str
    criticality:     int  = 5
    environment:     str  = "unknown"
    owner:           Optional[str] = None
    internet_facing: bool = False


class IdentityContext(BaseModel):
    user:             str
    is_privileged:    bool = False
    is_service_acct:  bool = False
    recent_offboard:  bool = False
    risk_score:       int  = 0


# ── Gold: fully assessed event ────────────────────────────────────────────────

class GoldEvent(BaseModel):
    """
    Fully enriched and AI-assessed event. Written to:
      - MinIO gold layer
      - Elasticsearch (hot, 30-90 days)
      - PostgreSQL (warm, forever)
    """
    silver:              SilverEvent

    # Enrichment
    ip_reputation:       Optional[IPReputation]   = None
    asset_context:       Optional[AssetContext]   = None
    identity_context:    Optional[IdentityContext] = None

    # Scoring
    threat_score:        int   = 0
    score_breakdown:     dict[str, int] = {}

    # LLM assessment
    recommended_action:  Optional[ActionTier] = None
    narrative:           Optional[str]        = None
    mitre_tactic:        Optional[str]        = None

    # Pipeline metadata
    enrichment_version:  str  = "1.0"
    processing_time_ms:  Optional[int] = None
    routed:              bool = False


# ── LangGraph agent state ─────────────────────────────────────────────────────

class AgentState(BaseModel):
    silver:              SilverEvent

    ip_reputation:       Optional[IPReputation]    = None
    asset_context:       Optional[AssetContext]     = None
    identity_context:    Optional[IdentityContext]  = None

    threat_score:        int            = 0
    score_breakdown:     dict[str, int] = {}

    recommended_action:  Optional[ActionTier] = None
    narrative:           Optional[str]        = None
    mitre_tactic:        Optional[str]        = None

    error:               Optional[str]        = None
    routed:              bool                 = False


# ── Dead letter event ─────────────────────────────────────────────────────────

class DeadLetterEvent(BaseModel):
    original_message: str
    error:            str
    stage:            str   # parse | clean | enrich
    timestamp:        str
    topic:            str   # source topic


# ── Aggregation event (Flink output) ─────────────────────────────────────────

class BaselineStats(BaseModel):
    """Statistics block emitted by the Flink baseline deviation detector."""
    current_count:       int
    rolling_average:     float
    multiplier:          float
    threshold:           float
    windows_in_baseline: int


class AggregationEvent(BaseModel):
    """
    A Flink-produced aggregation of silver events.
    Written to Postgres aggregations table and Elasticsearch security-aggregations.
    """
    id:                 str
    aggregation_type:   str

    source_ip:          Optional[str]  = None
    source_event_type:  Optional[str]  = None
    event_time:         Optional[str]  = None
    description:        Optional[str]  = None

    event_count:        Optional[int]  = None
    first_seen:         Optional[str]  = None
    last_seen:          Optional[str]  = None
    window_minutes:     Optional[int]  = None
    unique_dest_ips:    Optional[int]  = None
    unique_dest_ports:  Optional[int]  = None
    total_bytes_sent:   Optional[int]  = None
    velocity_key:       Optional[str]  = None

    baseline_stats:     Optional[BaselineStats] = None
    triggering_event:   Optional[dict]  = None

    pattern:            Optional[str]  = None
    chain:              Optional[dict] = None

    aggregated:         bool           = True
    correlated:         bool           = False

    enrichment_summary: Optional[dict] = None


# ── Enrichment summary sub-model ─────────────────────────────────────────────

class EnrichmentSummary(BaseModel):
    """
    Enrichment context borrowed from matching gold events.
    Populated by flink_consumer — no new API calls or LLM inference.
    All values are derived from existing gold events for the same
    source_ip within the aggregation's time window.
    """
    max_threat_score:     Optional[int]   = None
    avg_threat_score:     Optional[int]   = None
    recommended_action:   Optional[str]   = None
    mitre_tactic:         Optional[str]   = None
    narrative:            Optional[str]   = None
    ip_reputation:        Optional[dict]  = None
    asset_context:        Optional[dict]  = None
    identity_context:     Optional[dict]  = None
    source_event_ids:     list[str]       = []
    gold_events_found:    int             = 0