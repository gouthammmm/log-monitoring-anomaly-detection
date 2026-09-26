CREATE DATABASE IF NOT EXISTS logs;

CREATE TABLE IF NOT EXISTS logs.raw_logs
(
    ts          DateTime64(3),
    service     String,
    severity    LowCardinality(String),   -- INFO, WARN, ERROR
    message     String,
    latency_ms  Float32,
    trace_id    String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (service, ts);

CREATE TABLE IF NOT EXISTS logs.anomalies
(
    id              String,
    detected_at     DateTime64(3),
    window_start    DateTime64(3),
    window_end      DateTime64(3),
    service         String,
    metric          String,      -- e.g. "error_rate", "latency_p95"
    value           Float32,
    threshold       Float32,
    summary         String DEFAULT '',
    suggested_steps String DEFAULT ''
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(detected_at)
ORDER BY (service, detected_at);
