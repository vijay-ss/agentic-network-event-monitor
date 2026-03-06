from __future__ import annotations
"""
Yes, several good ones with generous free tiers:
IP Reputation

AbuseIPDB is the best free option for IP scoring. Free tier gives you 1,000 checks/day, 
returns an abuse confidence score (0-100), threat categories, and country. 
Perfect drop-in for your enrich_ip_reputation function.
VirusTotal gives you 500 requests/day free. You can check IPs, domains, file hashes, 
and URLs — broader than AbuseIPDB but the rate limit is tighter, so better used selectively for 
high-score events rather than every log entry.
IPinfo offers 50,000 requests/month free with geolocation, ASN, and basic threat data.
Good for the country/org context layer even if you use AbuseIPDB for the abuse score.
Threat Intelligence

AlienVault OTX (Open Threat Exchange) is completely free with no meaningful rate limit. 
It gives you indicators of compromise (IOCs), threat pulses, and MITRE ATT&CK mappings for IPs, domains, and file hashes. 
This one is particularly valuable for your use case because it maps directly to the MITRE tactic your 
LLM is already trying to identify — you could use OTX to pre-populate that field before the LLM even runs.

Shodan has a free tier that lets you look up what ports and services are exposed on an IP. 
Useful context for your asset enrichment — if the source IP is running an exposed Redis instance or has known vulnerabilities, that's meaningful signal.

DNS & Domain
URLScan.io is free and lets you look up domains for malicious indicators, screenshots, and associated IPs. Good for log entries involving outbound connections to suspicious domains.

Here's how I'd prioritize them for your pipeline given the rate limits:
Run AbuseIPDB on every log entry with a source IP — it's fast, simple, and the 1,000/day 
limit is fine for a small-medium scale project. Add AlienVault OTX as a second enrichment pass for any event that 
scores above your medium threshold, since it gives you richer IOC context without burning rate limit on noise. 
Use VirusTotal only for critical/high events or when you have a file hash to check, to stay within the 500/day cap.
"""

"""
Enrichment tools called as LangGraph nodes.

Live integrations:
  - AbuseIPDB   (IP reputation)     → 1,000 checks/day free
  - AlienVault OTX (threat intel)   → free, no hard rate limit
  - IPinfo      (geo / ASN context) → 50,000 req/month free

Selective integrations (called only for high/critical events):
  - VirusTotal  (deep IP check)     → 500 req/day free

Set the following environment variables (or add to a .env file):
  ABUSEIPDB_API_KEY
  OTX_API_KEY
  IPINFO_TOKEN
  VIRUSTOTAL_API_KEY (optional)
"""
import os
import logging
import asyncio
import aiohttp
from shared.models import AgentState, IPReputation, AssetContext, IdentityContext
from shared.config import config

ABUSEIPDB_KEY  = os.environ.get("ABUSEIPDB_API_KEY")
OTX_KEY        = os.environ.get("OTX_API_KEY")
IPINFO_TOKEN   = os.environ.get("IPINFO_TOKEN")
VIRUSTOTAL_KEY = os.environ.get("VIRUSTOTAL_API_KEY")

async def enrich_ip_reputation(state: AgentState) -> dict:
    ip = state.silver.source_ip
    if not ip:
        return {}
    
    abuse_data = await _query_abuseipdb(ip)
    ipinfo_data = await _query_ipinfo(ip)

    threat_types: list[str] = []
    if abuse_data.get("categories"):
        threat_types += _map_abuseipdb_categories(abuse_data["categories"])
    
    reputation = IPReputation(
        ip=ip,
        is_malicious=abuse_data.get("abuseConfidenceScore", 0) >= 25,
        threat_types=threat_types,
        country=abuse_data.get("countryCode") or ipinfo_data.get("country"),
        abuse_score=abuse_data.get("abuseConfidenceScore", 0),
        source="abuseipdb,ipinfo"
    )

    return {"ip_reputation": reputation}


async def _query_abuseipdb(ip: str) -> dict:
    if not ABUSEIPDB_KEY:
        return {}

    url = "https://api.abuseipdb.com/api/v2/check"
    headers = {"Key": ABUSEIPDB_KEY, "Accept": "application/json"}
    params  = {"ipAddress": ip, "maxAgeInDays": 90, "verbose": 1}
    timeout = aiohttp.ClientTimeout(total=8)

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers, params=params) as r:
                return (await r.json()).get("data", {}) if r.status == 200 else {}
    except Exception as e:
        logging.exception(e)
        return {}


async def _query_ipinfo(ip: str) -> dict:
    if not IPINFO_TOKEN:
        return {}
    url = f"https://ipinfo.io/{ip}/json"
    params = {"token": IPINFO_TOKEN}
    timeout = aiohttp.ClientTimeout(total=8)

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params=params) as r:
                if r.status == 200:
                    return await r.json() if r.status == 200 else {}
    except Exception as e:
        logging.exception(e)
        return {}


def _map_abuseipdb_categories(category_ids: list[int]) -> list[str]:
    """Map AbuseIPDB numeric category IDs to human-readable labels.
    Docs: https://www.abuseipdb.com/categories
    """
    mapping = {
        3:  "fraud_orders",      4:  "ddos_attack",
        5:  "ftp_brute_force",   6:  "ping_of_death",
        7:  "phishing",          8:  "fraud_voip",
        9:  "open_proxy",        10: "web_spam",
        11: "email_spam",        12: "blog_spam",
        13: "vpn_ip",            14: "port_scan",
        15: "hacking",           16: "sql_injection",
        17: "spoofing",          18: "brute_force",
        19: "bad_web_bot",       20: "exploited_host",
        21: "web_attack",        22: "ssh_brute_force",
        23: "iot_targeted",
    }

    return [mapping[c] for c in category_ids if c in mapping]


async def enrich_otx_threat_intel(ip: str) -> dict:
    """
    Returns OTX pulse count, threat tags, and MITRE ATT&CK IDs for an IP.
    Free with no hard rate limit.
    Docs: https://otx.alienvault.com/api
    """
    if not OTX_KEY or not ip:
        return {}
    
    url = f"https://otx.alienvault.com/api/v1/indicators/IPv4/{ip}/general"
    headers = {"X-OTX-API-KEY": OTX_KEY}
    timeout = aiohttp.ClientTimeout(total=30)

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers) as r:
                if r.status != 200:
                    return {}
                data = await r.json()
                pulses = data.get("pulse_info", {}).get("pulses", [])
                tags: set[str] = set()
                attack: set[str] = set()

                for p in pulses[:10]:
                    tags.update(p.get("tags", []))
                    for ref in p.get("attack_ids", []):
                        attack.add(ref.get("display_name", ""))
                
                return {
                    "otx_pulse_count": data.get("pulse_info", {}).get("count", 0),
                    "otx_tags": list(tags),
                    "otx_attack_ids": [a for a in attack if a],
                }
    except Exception as e:
        logging.exception(e)
        return {}


async def enrich_virustotal(ip: str) -> dict:
    if not VIRUSTOTAL_KEY or not ip:
        return {}
    
    url = f"https://www.virustotal.com/api/v3/ip_addresses/{ip}"
    headers = headers={"x-apikey": VIRUSTOTAL_KEY}
    timeout = aiohttp.ClientTimeout(total=8)

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers) as r:
                if r.status != 200:
                    return {}
                stats = (await r.json()).get("data", {}).get("attibutes", {}).get("last_analysis_stats", {})

                return {"vt_malicious": stats.get("malicious", 0)}
    except Exception as e:
        logging.exception(e)
        return {}


async def enrich_asset_context(state: AgentState) -> dict:
    host = state.silver.device_name or state.silver.destination_ip
    if not host:
        return {}
    # ------------------------------------------------------------------ #
    # SWAP: query your CMDB, ServiceNow, or AWS resource tags             #
    # ------------------------------------------------------------------ #
    prod_hosts     = {"auth-server-01", "db-primary", "payments-api"}
    internet_hosts = {"web-proxy", "vpn-gateway", "FW-DMZ-PRIMARY"}

    is_prod = (
        host in prod_hosts or
        "production" in (state.silver.asset_tags or [])
    )

    return {"asset_context": AssetContext(
        host=host,
        criticality=9 if is_prod else 4,
        environment="prod" if is_prod else "dev",
        internet_facing=host in internet_hosts or state.silver.security_zone == "untrusted",
        owner="platform-team" if is_prod else "dev-team",
    )}


async def enrich_identity_context(state: AgentState) -> dict:
    user = state.silver.user
    if not user:
        return {}
    # ------------------------------------------------------------------ #
    # SWAP: query Okta, Azure AD, or your internal IdP                    #
    # ------------------------------------------------------------------ #
    privileged_users = {"admin", "root", "svc-deploy", "svc-backup"}
    offboarded_users = {"john.doe.old", "contractor_expired"}

    return {"identity_context": IdentityContext(
        user=user,
        is_privileged=user in privileged_users,
        is_service_acct=user.startswith("svc-"),
        recent_offboard=user in offboarded_users,
        risk_score=85 if user in offboarded_users else (
                   60 if user in privileged_users else 10),
    )}



async def run_all_enrichments(state: AgentState) -> dict:
    high_signal = state.silver.severity.value in ("HIGH", "CRITICAL")

    tasks = [
        enrich_ip_reputation(state),
        enrich_asset_context(state),
        enrich_identity_context(state),
    ]

    if state.silver.source_ip and OTX_KEY:
        tasks.append(enrich_otx_threat_intel(state.silver.source_ip))
    
    if high_signal and state.silver.source_ip and VIRUSTOTAL_KEY:
        tasks.append(enrich_virustotal(state.silver.source_ip))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)

    merged: dict = {}
    otx_data: dict = {}

    for r in results:
        if not isinstance(r, dict):
            continue
        if "otx_pulse_count" in r:
            otx_data = r
        else:
            merged.update(r)
    
    if otx_data and "ip_reputation" in merged and merged["ip_reputation"]:
        rep = merged["ip_reputation"]
        rep.threat_types = list(set(rep.threat_types + otx_data.get("otx_tags", [])))
        if otx_data.get("otx_pulse_count", 0) > 0:
            rep.is_malicious = True
        merged["ip_reputation"] = rep
    
    return merged