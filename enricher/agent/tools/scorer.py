from __future__ import annotations
from shared.models import AgentState, Severity, Action

SEVERITY_BASE = {
    Severity.INFO:      5,
    Severity.LOW:       15,
    Severity.MEDIUM:    35,
    Severity.HIGH:      60,
    Severity.CRITICAL:  85,
}

EVENT_TYPE_BOOST = {
    "FAILED_LOGIN":         5,
    "SSH_ATTEMPT":          10,
    "BRUTE_FORCE":          20,
    "PRIVILEGE_ESCALATION": 30,
    "LATERAL_MOVEMENT":     25,
    "DATA_EXFILTRATION":    35,
    "PORT_SCAN":            10,
    "MALWARE_DETECTED":     40,
    "CONFIG_CHANGE":        8,
    "NEW_ADMIN_USER":       20,
}


def score_event(state: AgentState) -> dict:
    silver = state.silver
    ip = state.ip_reputation
    asset = state.asset_context
    ident = state.identity_context
    breakdown: dict[str, int] = {}

    # 1. Severity base
    base = SEVERITY_BASE.get(silver.severity, 10)
    breakdown["severity_base"] = base

    # 2. Event type boost
    event = EVENT_TYPE_BOOST.get(silver.event_type.upper(), 0)
    breakdown["event_type_boost"] = event

    # 3. Action context - blocked events score lower than allowed
    action_mod = 0
    if silver.action == Action.ALLOW and silver.severity != Severity.INFO:
        action_mod = 5
    breakdown["action_modifier"] = action_mod

    # 4. Policy violation
    policy_score = 0
    if silver.policy_violation:
        sev = (silver.policy_violation.policy_severity or "").lower()
        policy_score = {"low": 5, "medium": 15, "high": 25}.get(sev, 5)
    breakdown["policy_violation"] = policy_score

    # 5. Security zone
    zone_score = 0
    if silver.security_zone == "untrusted":
        zone_score = 10
    breakdown["security_zone"] = zone_score

    # 6. IP reputation
    ip_score = 0
    if ip and ip.is_malicious:
        ip_score = min(ip.abuse_score // 2, 30)
    breakdown["ip_reputation"] = ip_score

    # 7. Asset criticality
    asset_score = 0
    if asset:
        asset_score = int((asset.criticality / 10) * 20)
        if asset.internet_facing:
            asset_score += 5
    breakdown["asset_criticality"] = asset_score

    # 8. Identity risk
    ident_score = 0
    if ident:
        if ident.recent_offboard:
            ident_score += 25
        if ident.is_privileged:
            ident_score += 10
        ident_score += ident.risk_score // 10
    breakdown["identity_risk"] = ident_score

    composite = min(
        base
        + event
        + action_mod
        + policy_score
        + zone_score
        + ip_score
        + asset_score
        + ident_score,
        100
    )
    breakdown["composite"] = composite

    return {"threat_score": composite, "score_breakdown": breakdown}