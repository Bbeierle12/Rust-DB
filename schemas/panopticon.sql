-- Panopticon schema for Rust-DB
--
-- Type mapping:
--   TEXT           → Value::Text
--   BIGINT         → Value::Int64
--   DOUBLE PRECISION → Value::Float64
--   BOOLEAN        → Value::Bool
--   TIMESTAMP      → Value::Timestamp (microseconds since Unix epoch)
--
-- Conventions:
--   * The FIRST column is the primary key (Rust-DB stores rows under that key).
--   * DEFAULT constraints are accepted by the SQL parser but NOT enforced by
--     the engine — applications must supply every column value on INSERT.
--   * Timestamp literals: TIMESTAMP '2024-01-15 10:30:00'
--
-- Execute one statement at a time via Database::execute_sql().

-- ─── devices ────────────────────────────────────────────────────────────────

CREATE TABLE devices (
    id          TEXT      NOT NULL,
    ip          TEXT      NOT NULL,
    mac         TEXT,
    hostname    TEXT,
    vendor      TEXT,
    os_family   TEXT,
    os_version  TEXT,
    device_type TEXT,
    status      TEXT,
    notes       TEXT,
    first_seen  TIMESTAMP,
    last_seen   TIMESTAMP
);

CREATE UNIQUE INDEX idx_devices_ip  ON devices(ip);
CREATE INDEX        idx_devices_mac ON devices(mac);

-- ─── ports ──────────────────────────────────────────────────────────────────

CREATE TABLE ports (
    id              TEXT   NOT NULL,
    device_id       TEXT   NOT NULL,
    port_number     BIGINT NOT NULL,
    protocol        TEXT,
    state           TEXT,
    service_name    TEXT,
    service_version TEXT,
    banner          TEXT,
    first_seen      TIMESTAMP,
    last_seen       TIMESTAMP
);

CREATE UNIQUE INDEX idx_ports_device_port ON ports(device_id, port_number, protocol);

-- ─── alerts ─────────────────────────────────────────────────────────────────
-- Note: DEFAULT 1 on `count` is not enforced; INSERT must supply the value.

CREATE TABLE alerts (
    id             TEXT   NOT NULL,
    severity       TEXT   NOT NULL,
    status         TEXT   NOT NULL,
    source_tool    TEXT,
    category       TEXT,
    title          TEXT,
    description    TEXT,
    device_ip      TEXT,
    fingerprint    TEXT,
    correlation_id TEXT,
    count          BIGINT,
    raw_data       TEXT,
    first_seen     TIMESTAMP,
    last_seen      TIMESTAMP,
    created_at     TIMESTAMP,
    updated_at     TIMESTAMP
);

CREATE INDEX idx_alerts_severity    ON alerts(severity);
CREATE INDEX idx_alerts_status      ON alerts(status);
CREATE INDEX idx_alerts_fingerprint ON alerts(fingerprint);
CREATE INDEX idx_alerts_device_ip   ON alerts(device_ip);

-- ─── scans ──────────────────────────────────────────────────────────────────

CREATE TABLE scans (
    id           TEXT             NOT NULL,
    scan_type    TEXT,
    tool         TEXT,
    target       TEXT,
    status       TEXT,
    progress     DOUBLE PRECISION,
    parameters   TEXT,
    results      TEXT,
    started_at   TIMESTAMP,
    completed_at TIMESTAMP,
    created_at   TIMESTAMP
);

-- ─── vulnerabilities ────────────────────────────────────────────────────────

CREATE TABLE vulnerabilities (
    id              TEXT             NOT NULL,
    cve_id          TEXT,
    cvss_score      DOUBLE PRECISION,
    severity        TEXT,
    title           TEXT,
    description     TEXT,
    device_id       TEXT,
    device_ip       TEXT,
    port            BIGINT,
    source_tool     TEXT,
    service         TEXT,
    solution        TEXT,
    status          TEXT,
    references_json TEXT,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

CREATE INDEX idx_vulns_severity ON vulnerabilities(severity);
CREATE INDEX idx_vulns_device   ON vulnerabilities(device_id);

-- ─── traffic_flows ──────────────────────────────────────────────────────────

CREATE TABLE traffic_flows (
    id               TEXT   NOT NULL,
    src_ip           TEXT,
    src_port         BIGINT,
    dst_ip           TEXT,
    dst_port         BIGINT,
    protocol         TEXT,
    bytes_sent       BIGINT,
    bytes_received   BIGINT,
    packets_sent     BIGINT,
    packets_received BIGINT,
    first_seen       TIMESTAMP,
    last_seen        TIMESTAMP
);

-- ─── scheduled_jobs ─────────────────────────────────────────────────────────

CREATE TABLE scheduled_jobs (
    id           TEXT    NOT NULL,
    trigger_type TEXT,
    trigger_args TEXT,
    task_type    TEXT,
    task_params  TEXT,
    enabled      BOOLEAN,
    created_at   TIMESTAMP,
    updated_at   TIMESTAMP
);
