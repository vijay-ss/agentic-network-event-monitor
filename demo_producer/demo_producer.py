# scripts/demo_producer.py
"""
Produces synthetic log entries matching your real log schema
directly to logs.bronze.events.v1 for local testing.

Usage:
    python scripts/demo_producer.py           # sends all samples once
    python scripts/demo_producer.py --loop    # streams continuously
    python scripts/demo_producer.py --count 50  # sends N random events
"""
import os
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC = "logs.bronze.events.v1"

SAMPLE_LOGS = [
    {
        "id": None,   # filled at send time
        "timestamp": None,
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
        "user_agent": "Mozilla/5.0 Chrome/121.0.0.0",
        "log_metadata": {"collector": "splunk-forwarder-03", "source_type": "firewall", "ingestion_time": None, "pipeline": "network_events"},
        "network_details": {"network_segment": "CORPORATE_LAN", "subnet": "192.168.1.0/24", "security_zone": "trusted"},
        "connection_state": "ESTABLISHED",
        "tcp_flags": "ACK,PSH",
        "policy_violation": None,
        "asset_tags": ["production", "standard_workstation"],
        "session_details": None,
        "resource_utilization": None,
        "response_code": 200,
        "threat_indicators": [],
        "rule_id": "FW-ALLOW-001",
        "description": "User accessing Google Docs",
    },
    {
        "id": None,
        "timestamp": None,
        "source_ip": "185.220.101.47",
        "destination_ip": "10.0.2.5",
        "destination_domain": None,
        "source_port": 44892,
        "destination_port": 22,
        "protocol": "TCP",
        "application": "SSH",
        "action": "BLOCK",
        "bytes_sent": 0,
        "bytes_received": 0,
        "packet_count": 1,
        "duration_ms": 0,
        "event_type": "SSH_ATTEMPT",
        "severity": "HIGH",
        "user": None,
        "correlation_id": None,
        "device_name": "FW-DMZ-PRIMARY",
        "vlan_id": 1,
        "geo_location": "Frankfurt, DE",
        "user_agent": "OpenSSH_8.9p1",
        "log_metadata": {"collector": "palo-alto-collector-01", "source_type": "firewall", "ingestion_time": None, "pipeline": "perimeter_security"},
        "network_details": {"network_segment": "DMZ", "subnet": "10.0.2.0/24", "security_zone": "untrusted"},
        "connection_state": "CLOSED",
        "tcp_flags": "SYN",
        "policy_violation": {"policy_name": "CORP-FW-DMZ-001", "violation_type": "unauthorized_protocol", "policy_severity": "high"},
        "asset_tags": ["DMZ_server", "production"],
        "session_details": None,
        "resource_utilization": None,
        "response_code": None,
        "threat_indicators": [],
        "rule_id": "FW-BLOCK-SSH-EXT",
        "description": "External SSH connection attempt blocked",
    },
    {
        "id": None,
        "timestamp": None,
        "source_ip": "192.168.1.91",
        "destination_ip": "140.82.121.6",
        "destination_domain": "github.com",
        "source_port": 49823,
        "destination_port": 443,
        "protocol": "TCP",
        "application": "HTTPS",
        "action": "ALLOW",
        "bytes_sent": 4096,
        "bytes_received": 16384,
        "packet_count": 89,
        "duration_ms": 2341,
        "event_type": "HTTPS_CONNECTION",
        "severity": "INFO",
        "user": "kate.davis",
        "correlation_id": "corr_git_e7a2",
        "device_name": "LAPTOP-ENG-KDAVIS",
        "vlan_id": 150,
        "geo_location": "Toronto, ON, CA",
        "user_agent": "git/2.43.0",
        "log_metadata": {"collector": "crowdstrike-edr-agent", "source_type": "edr_agent", "ingestion_time": None, "pipeline": "endpoint_security"},
        "network_details": {"network_segment": "CORPORATE_LAN", "subnet": "192.168.1.0/24", "security_zone": "trusted"},
        "connection_state": "ESTABLISHED",
        "tcp_flags": "ACK",
        "policy_violation": None,
        "asset_tags": ["development", "engineering_dept"],
        "session_details": None,
        "resource_utilization": {"cpu_percent": 12.3, "memory_percent": 34.7, "disk_io_percent": 8.2, "process_name": "git.exe", "process_id": 7823},
        "response_code": 200,
        "threat_indicators": [],
        "rule_id": "FW-ALLOW-DEV-HTTPS",
        "description": "Developer syncing code repository",
    },
    # High severity: simulated brute force
    {
        "id": None,
        "timestamp": None,
        "source_ip": "45.142.212.100",
        "destination_ip": "10.0.1.10",
        "destination_domain": None,
        "source_port": 31337,
        "destination_port": 22,
        "protocol": "TCP",
        "application": "SSH",
        "action": "BLOCK",
        "bytes_sent": 512,
        "bytes_received": 0,
        "packet_count": 8,
        "duration_ms": 150,
        "event_type": "BRUTE_FORCE",
        "severity": "CRITICAL",
        "user": "admin",
        "correlation_id": "corr_bf_001",
        "device_name": "auth-server-01",
        "vlan_id": 1,
        "geo_location": "Kyiv, UA",
        "user_agent": "libssh2",
        "log_metadata": {"collector": "palo-alto-collector-01", "source_type": "firewall", "ingestion_time": None, "pipeline": "perimeter_security"},
        "network_details": {"network_segment": "DMZ", "subnet": "10.0.1.0/24", "security_zone": "untrusted"},
        "connection_state": "CLOSED",
        "tcp_flags": "SYN,ACK",
        "policy_violation": {"policy_name": "CORP-IDS-001", "violation_type": "brute_force_detected", "policy_severity": "high"},
        "asset_tags": ["production", "internet_facing"],
        "session_details": None,
        "resource_utilization": None,
        "response_code": None,
        "threat_indicators": ["known_bad_ip", "brute_force_pattern"],
        "rule_id": "IDS-BF-001",
        "description": "Repeated SSH authentication failures from external IP",
    },
]


def send_log(producer: Producer, log: dict):
    log = log.copy()
    now = datetime.now(timezone.utc).isoformat()
    log["id"] = f"log_{uuid.uuid4().hex[:8]}"
    log["timestamp"] = now
    if log.get("log_metadata"):
        log["log_metadata"]["ingestion_time"] = now

    producer.produce(
        topic=TOPIC,
        key=log["id"].encode(),
        value=json.dumps(log).encode(),
    )
    producer.poll(0)
    print(f"→ {log['event_type']:30s} | {log['severity']:8s} | {log['source_ip']}")


def main():
    parser = argparse.ArgumentParser(description="Security log demo producer")
    parser.add_argument("--loop",  action="store_true", help="Stream continuously")
    parser.add_argument("--count", type=int, default=0, help="Send N random events")
    parser.add_argument("--delay", type=float, default=0, help="Seconds between events in loop mode")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": KAFKA_SERVERS})
    print(f"Producing to topic: {TOPIC}\n")

    if args.count > 0:
        for _ in range(args.count):
            send_log(producer, random.choice(SAMPLE_LOGS))
            time.sleep(args.delay)
    elif args.loop:
        print("Streaming continuously (Ctrl+C to stop)...\n")
        try:
            while True:
                send_log(producer, random.choice(SAMPLE_LOGS))
                time.sleep(args.delay)
        except KeyboardInterrupt:
            pass
    else:
        for log in SAMPLE_LOGS:
            send_log(producer, log)

    producer.flush()
    print(f"\nDone.")


if __name__ == "__main__":
    main()