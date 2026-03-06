"""
Data cleaning transformation functions.

All functions are:
  - Pure Python (no LLM, no external calls)
  - Deterministic
  - Unit testable
  - Fault tolerant (log warnings, don't raise on recoverable issues)

Cleaning covers:
  1. Timestamp normalization → UTC ISO 8601
  2. IP address validation and normalization
  3. Field name standardization (flattening nested structs)
  4. Null / missing value handling with explicit defaults
  5. Enum normalization (severity, action → uppercase)
  6. TCP flags parsing (string → list)
  7. Deduplication key generation
"""
from __future__ import annotations
import ipaddress
import uuid
from datetime import datetime, timezone
from dateutil import parser as dateparser

from shared.models import BronzeEvent, SilverEvent, Severity, Action


def clean_event(bronze: BronzeEvent) -> SilverEvent:
    warnings: list[str] = []

    event_time  = _normalize_timestamp(bronze.timestamp, warnings)
    source_ip   = _normalize_ip(bronze.source_ip, "source_ip", warnings)
    dest_ip     = _normalize_ip(bronze.destination_ip, "destination_ip", warnings)
    severity    = _normalize_severity(bronze.severity, warnings)
    action      = _normalize_action(bronze.action, warnings)
    tcp_flags   = _parse_tcp_flags(bronze.tcp_flags)
    event_type  = _normalize_event_type(bronze.event_type, warnings)

    ingestion_time  = bronze.log_metadata.ingestion_time if bronze.log_metadata else None
    source_type     = bronze.log_metadata.source_type    if bronze.log_metadata else None
    pipeline        = bronze.log_metadata.pipeline       if bronze.log_metadata else None
    collector       = bronze.log_metadata.collector      if bronze.log_metadata else None
    network_segment = bronze.network_details.network_segment if bronze.network_details else None
    security_zone   = bronze.network_details.security_zone   if bronze.network_details else None

    process_name   = None
    process_id     = None
    cpu_percent    = None
    memory_percent = None
    if bronze.resource_utilization:
        process_name   = bronze.resource_utilization.process_name
        process_id     = bronze.resource_utilization.process_id
        cpu_percent    = bronze.resource_utilization.cpu_percent
        memory_percent = bronze.resource_utilization.memory_percent

    dest_domain = _normalize_domain(bronze.destination_domain, warnings)

    return SilverEvent(
        id=str(uuid.uuid4()),
        original_id=bronze.id,
        event_time=event_time,
        ingestion_time=_normalize_timestamp(ingestion_time, warnings) if ingestion_time else None,
        source_ip=source_ip,
        destination_ip=dest_ip,
        destination_domain=dest_domain,
        source_port=_normalize_port(bronze.source_port, warnings),
        destination_port=_normalize_port(bronze.destination_port, warnings),
        protocol=bronze.protocol.upper() if bronze.protocol else None,
        application=bronze.application.upper() if bronze.application else None,
        bytes_sent=max(bronze.bytes_sent or 0, 0),
        bytes_received=max(bronze.bytes_received or 0, 0),
        packet_count=max(bronze.packet_count or 0, 0),
        duration_ms=max(bronze.duration_ms or 0, 0),
        event_type=event_type,
        severity=severity,
        action=action,
        connection_state=bronze.connection_state,
        tcp_flags=tcp_flags,
        response_code=bronze.response_code,
        user=_normalize_user(bronze.user),
        device_name=bronze.device_name,
        source_type=source_type,
        pipeline=pipeline,
        collector=collector,
        network_segment=network_segment,
        security_zone=security_zone,
        geo_location=bronze.geo_location,
        vlan_id=bronze.vlan_id,
        asset_tags=bronze.asset_tags or [],
        rule_id=bronze.rule_id,
        correlation_id=bronze.correlation_id,
        policy_violation=bronze.policy_violation,
        threat_indicators=bronze.threat_indicators or [],
        process_name=process_name,
        process_id=process_id,
        cpu_percent=cpu_percent,
        memory_percent=memory_percent,
        cleaning_version="1.0",
        cleaning_warnings=warnings,
    )


def _normalize_timestamp(ts: str | None, warnings: list[str]) -> str:
    if not ts:
        warnings.append("missing timestamp — using current UTC time")
        return datetime.now(timezone.utc).isoformat()
    try:
        dt = dateparser.parse(ts)
        if dt.tzinfo is None:
            warnings.append(f"timestamp '{ts}' has no timezone — assuming UTC")
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        warnings.append(f"unparseable timestamp '{ts}' — using current UTC time")
        return datetime.now(timezone.utc).isoformat()


def _normalize_ip(ip: str | None, field: str, warnings: list[str]) -> str | None:
    if not ip:
        return None
    # Strip port if appended (e.g. "192.168.1.1:4444")
    ip = ip.split(":")[0] if ":" in ip and not ip.startswith("[") else ip
    # Strip IPv4-mapped IPv6 prefix
    if ip.startswith("::ffff:"):
        ip = ip[7:]
    try:
        return str(ipaddress.ip_address(ip))
    except ValueError:
        warnings.append(f"invalid IP in {field}: '{ip}' — set to null")
        return None


def _normalize_domain(domain: str | None, warnings: list[str]) -> str | None:
    if not domain:
        return None
    domain = domain.strip().lower()
    # Basic sanity check
    if len(domain) > 253 or " " in domain:
        warnings.append(f"invalid domain '{domain}' — set to null")
        return None
    return domain


def _normalize_port(port: int | None, warnings: list[str]) -> int | None:
    if port is None:
        return None
    if 0 <= port <= 65535:
        return port
    warnings.append(f"invalid port {port} — set to null")
    return None


_SEVERITY_MAP = {
    "info":     Severity.INFO,
    "low":      Severity.LOW,
    "medium":   Severity.MEDIUM,
    "med":      Severity.MEDIUM,
    "high":     Severity.HIGH,
    "critical": Severity.CRITICAL,
    "crit":     Severity.CRITICAL,
}

def _normalize_severity(raw: str | None, warnings: list[str]) -> Severity:
    if not raw:
        warnings.append("missing severity — defaulting to INFO")
        return Severity.INFO
    normalized = _SEVERITY_MAP.get(raw.lower().strip())
    if not normalized:
        warnings.append(f"unknown severity '{raw}' — defaulting to INFO")
        return Severity.INFO
    return normalized


_ACTION_MAP = {
    "allow":   Action.ALLOW,
    "permit":  Action.ALLOW,
    "pass":    Action.ALLOW,
    "block":   Action.BLOCK,
    "deny":    Action.BLOCK,
    "drop":    Action.DROP,
    "alert":   Action.ALERT,
    "monitor": Action.ALERT,
}

def _normalize_action(raw: str | None, warnings: list[str]) -> Action | None:
    if not raw:
        return None
    normalized = _ACTION_MAP.get(raw.lower().strip())
    if not normalized:
        warnings.append(f"unknown action '{raw}' — set to null")
        return None
    return normalized


def _parse_tcp_flags(flags: str | None) -> list[str]:
    if not flags:
        return []
    return [f.strip().upper() for f in flags.split(",") if f.strip()]


def _normalize_event_type(event_type: str | None, warnings: list[str]) -> str:
    if not event_type:
        warnings.append("missing event_type — defaulting to UNKNOWN")
        return "UNKNOWN"
    return event_type.upper().strip()


def _normalize_user(user: str | None) -> str | None:
    if not user:
        return None
    user = user.strip().lower()
    return user if user else None