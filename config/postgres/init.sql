-- Security pipeline schema
-- Tables:
--   events           individual gold events from the enricher agent
--                    RENAMED from assessed_events
--   aggregations     Flink-produced aggregations (velocity, CEP chains, baseline)
--                    NEW — replaces the planned flink_alerts table
--   dead_letter      failed events from all pipeline stages
--
-- Views:
--   v_hourly_threat_trends    hourly event counts and score trends
--   v_top_threat_ips          per-IP threat summary
--   v_mitre_coverage          MITRE ATT&CK tactic distribution
--   v_action_summary          daily action tier breakdown
--   v_aggregation_enriched    JOIN view: aggregations + enrichment from events
--   v_ip_full_picture         JOIN view: all activity per IP across both tables

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


-- ── Individual enriched events ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS events (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    silver_id           TEXT NOT NULL UNIQUE,
    original_id         TEXT,

    -- Timing
    event_time          TIMESTAMPTZ NOT NULL,
    ingestion_time      TIMESTAMPTZ,
    processed_at        TIMESTAMPTZ DEFAULT NOW(),
    processing_time_ms  INT,

    -- Event classification
    event_type          TEXT,
    severity            TEXT,
    action              TEXT,
    protocol            TEXT,
    application         TEXT,
    connection_state    TEXT,
    response_code       INT,
    rule_id             TEXT,

    -- Network
    source_ip           TEXT,
    destination_ip      TEXT,
    destination_domain  TEXT,
    source_port         INT,
    destination_port    INT,
    bytes_sent          BIGINT DEFAULT 0,
    bytes_received      BIGINT DEFAULT 0,
    packet_count        INT    DEFAULT 0,
    duration_ms         INT    DEFAULT 0,
    tcp_flags           TEXT[],

    -- Identity
    user_name           TEXT,
    device_name         TEXT,

    -- Context
    source_type         TEXT,
    pipeline            TEXT,
    collector           TEXT,
    network_segment     TEXT,
    security_zone       TEXT,
    geo_location        TEXT,
    vlan_id             INT,
    asset_tags          TEXT[],
    correlation_id      TEXT,

    -- Endpoint (EDR)
    process_name        TEXT,
    process_id          INT,
    cpu_percent         FLOAT,
    memory_percent      FLOAT,

    -- Policy
    policy_name         TEXT,
    policy_violation_type TEXT,
    policy_severity     TEXT,

    -- AI enrichment
    threat_score        INT,
    recommended_action  TEXT,
    mitre_tactic        TEXT,
    narrative           TEXT,

    -- Flexible storage for full detail
    score_breakdown     JSONB,
    ip_reputation       JSONB,
    asset_context       JSONB,
    identity_context    JSONB,
    cleaning_warnings   TEXT[]
);

CREATE INDEX idx_events_event_time    ON events (event_time DESC);
CREATE INDEX idx_events_threat_score  ON events (threat_score DESC);
CREATE INDEX idx_events_event_type    ON events (event_type);
CREATE INDEX idx_events_action        ON events (recommended_action);
CREATE INDEX idx_events_source_ip     ON events (source_ip);
CREATE INDEX idx_events_severity      ON events (severity);
CREATE INDEX idx_events_mitre         ON events (mitre_tactic);
CREATE INDEX idx_events_user          ON events (user_name);
CREATE INDEX idx_events_device        ON events (device_name);
CREATE INDEX idx_events_ip_time       ON events (source_ip, event_time DESC);


-- ── Flink aggregations ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS aggregations (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    flink_id            TEXT UNIQUE NOT NULL,
    aggregation_type    TEXT NOT NULL,
    processed_at        TIMESTAMPTZ DEFAULT NOW(),

    -- Core fields shared across all aggregation types
    source_ip           TEXT,
    source_event_type   TEXT,
    event_time          TIMESTAMPTZ,
    first_seen          TIMESTAMPTZ,
    last_seen           TIMESTAMPTZ,
    window_minutes      INT,
    description         TEXT,

    -- Velocity / baseline fields
    event_count         INT,
    unique_dest_ips     INT,
    unique_dest_ports   INT,
    total_bytes_sent    BIGINT,

    -- Baseline-specific fields
    rolling_average     FLOAT,
    baseline_multiplier FLOAT,
    windows_in_baseline INT,

    -- CEP chain fields
    pattern             TEXT,
    chain               JSONB,

    -- Enrichment summary — populated by flink_consumer joining against events
    max_threat_score    INT,
    avg_threat_score    INT,
    recommended_action  TEXT,
    mitre_tactic        TEXT,
    narrative           TEXT,
    ip_reputation       JSONB,
    asset_context       JSONB,
    identity_context    JSONB,

    -- IDs of the individual events rows that contributed to this aggregation
    source_event_ids    TEXT[]
);

CREATE INDEX idx_agg_event_time       ON aggregations (event_time DESC);
CREATE INDEX idx_agg_type             ON aggregations (aggregation_type);
CREATE INDEX idx_agg_source_ip        ON aggregations (source_ip);
CREATE INDEX idx_agg_threat_score     ON aggregations (max_threat_score DESC);
CREATE INDEX idx_agg_action           ON aggregations (recommended_action);
CREATE INDEX idx_agg_ip_window        ON aggregations (source_ip, first_seen, last_seen);


-- ── Dead letter ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dead_letter (
    id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    received_at      TIMESTAMPTZ DEFAULT NOW(),
    stage            TEXT,
    source_topic     TEXT,
    error            TEXT,
    original_message TEXT
);


-- ── Views ─────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW v_hourly_threat_trends AS
SELECT
    DATE_TRUNC('hour', event_time)  AS hour,
    event_type,
    severity,
    recommended_action,
    COUNT(*)                        AS event_count,
    AVG(threat_score)::INT          AS avg_score,
    MAX(threat_score)               AS max_score,
    COUNT(*) FILTER (
        WHERE recommended_action IN ('page', 'contain')
    )                               AS high_severity_count
FROM events
GROUP BY 1, 2, 3, 4;


CREATE OR REPLACE VIEW v_top_threat_ips AS
SELECT
    source_ip,
    COUNT(*)                        AS event_count,
    AVG(threat_score)::INT          AS avg_score,
    MAX(threat_score)               AS max_score,
    ARRAY_AGG(DISTINCT event_type)  AS event_types,
    ARRAY_AGG(DISTINCT mitre_tactic) FILTER (
        WHERE mitre_tactic IS NOT NULL
    )                               AS mitre_tactics,
    MAX(event_time)                 AS last_seen
FROM events
WHERE source_ip IS NOT NULL
GROUP BY source_ip
ORDER BY avg_score DESC, event_count DESC;


CREATE OR REPLACE VIEW v_mitre_coverage AS
SELECT
    mitre_tactic,
    COUNT(*)           AS event_count,
    AVG(threat_score)  AS avg_score,
    MAX(event_time)    AS last_seen
FROM events
WHERE mitre_tactic IS NOT NULL
GROUP BY mitre_tactic
ORDER BY event_count DESC;


CREATE OR REPLACE VIEW v_action_summary AS
SELECT
    DATE_TRUNC('day', event_time)  AS day,
    recommended_action,
    COUNT(*)                       AS count
FROM events
GROUP BY 1, 2
ORDER BY 1 DESC, count DESC;


CREATE OR REPLACE VIEW v_aggregation_enriched AS
SELECT
    -- Aggregation fields
    a.flink_id,
    a.aggregation_type,
    a.source_ip,
    a.source_event_type,
    a.event_time,
    a.first_seen,
    a.last_seen,
    a.window_minutes,
    a.event_count,
    a.description,
    a.pattern,
    a.rolling_average,
    a.baseline_multiplier,

    -- Enrichment from the aggregation row itself (copied at write time)
    a.max_threat_score,
    a.avg_threat_score,
    a.recommended_action,
    a.mitre_tactic,
    a.narrative,
    a.ip_reputation,
    a.asset_context,
    a.identity_context,

    -- Full detail of the highest-scoring individual event in the same window
    e.id               AS top_event_id,
    e.event_type       AS top_event_type,
    e.threat_score     AS top_event_score,
    e.narrative        AS top_event_narrative,
    e.destination_ip   AS top_dest_ip,
    e.destination_port AS top_dest_port
FROM aggregations a
LEFT JOIN LATERAL (
    SELECT *
    FROM events e
    WHERE e.source_ip  = a.source_ip
      AND e.event_time >= a.first_seen
      AND e.event_time <= a.last_seen
    ORDER BY e.threat_score DESC
    LIMIT 1
) e ON true;


CREATE OR REPLACE VIEW v_ip_full_picture AS
SELECT
    source_ip,
    'event'                         AS record_type,
    id::TEXT                        AS record_id,
    event_time,
    event_type,
    threat_score,
    recommended_action,
    mitre_tactic,
    narrative,
    NULL::TEXT                      AS aggregation_type,
    NULL::INT                       AS event_count,
    NULL::TEXT                      AS pattern
FROM events
UNION ALL
SELECT
    source_ip,
    'aggregation'                   AS record_type,
    flink_id                        AS record_id,
    event_time,
    source_event_type               AS event_type,
    max_threat_score                AS threat_score,
    recommended_action,
    mitre_tactic,
    description                     AS narrative,
    aggregation_type,
    event_count,
    pattern
FROM aggregations
ORDER BY event_time DESC;