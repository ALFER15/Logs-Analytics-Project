-- Refresh the aggregated layer before running the fast analytical query
-- REFRESH MATERIALIZED VIEW mv_logs_hourly_endpoint;

DROP MATERIALIZED VIEW IF EXISTS mv_logs_hourly_endpoint;

CREATE MATERIALIZED VIEW mv_logs_hourly_endpoint AS
SELECT
    date_trunc('hour', la."timestamp") AS hour_bucket,
    la.endpoint_id,
    COUNT(*) AS total_requests,
    COUNT(*) FILTER (WHERE la.status_code >= 500) AS total_5xx,
    COUNT(*) FILTER (WHERE la.status_code >= 400 AND la.status_code < 500) AS total_4xx,
    COUNT(le.log_id) AS total_errors_joined,
    AVG(la.latency_ms)::numeric(10,2) AS avg_latency_ms,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY la.latency_ms)::numeric(10,2) AS p50_latency_ms,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY la.latency_ms)::numeric(10,2) AS p95_latency_ms,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY la.latency_ms)::numeric(10,2) AS p99_latency_ms
FROM logs_access la
LEFT JOIN logs_error le
    ON le.log_id = la.id
   AND le."timestamp" = la."timestamp"
GROUP BY 1, 2;

-- Query 1: executive performance snapshot
SELECT
    s.name AS service_name,
    e.method,
    e.path,
    mv.hour_bucket,
    mv.total_requests,
    mv.total_5xx,
    mv.total_4xx,
    mv.total_errors_joined,
    mv.avg_latency_ms,
    mv.p50_latency_ms,
    mv.p95_latency_ms,
    mv.p99_latency_ms,
    ROUND((mv.total_5xx::numeric / NULLIF(mv.total_requests, 0)) * 100, 2) AS error_5xx_rate_pct,
    ROUND((mv.total_4xx::numeric / NULLIF(mv.total_requests, 0)) * 100, 2) AS error_4xx_rate_pct,
    ROUND((mv.total_errors_joined::numeric / NULLIF(mv.total_requests, 0)) * 100, 2) AS joined_error_rate_pct
FROM mv_logs_hourly_endpoint mv
JOIN endpoints e ON e.id = mv.endpoint_id
JOIN services s ON s.id = e.service_id
WHERE mv.hour_bucket >= now() - interval '7 days'
ORDER BY mv.hour_bucket DESC, s.name, e.method, e.path
LIMIT 200;

-- Query 2: top endpoints by error rate and latency
SELECT
    s.name AS service_name,
    e.method,
    e.path,
    mv.hour_bucket,
    mv.total_requests,
    mv.total_5xx,
    mv.p95_latency_ms,
    ROUND((mv.total_5xx::numeric / NULLIF(mv.total_requests, 0)) * 100, 2) AS error_5xx_rate_pct,
    ROUND((mv.p95_latency_ms - mv.p50_latency_ms), 2) AS p95_minus_p50_ms
FROM mv_logs_hourly_endpoint mv
JOIN endpoints e ON e.id = mv.endpoint_id
JOIN services s ON s.id = e.service_id
WHERE mv.hour_bucket >= now() - interval '7 days'
ORDER BY error_5xx_rate_pct DESC, mv.p95_latency_ms DESC
LIMIT 20;

-- Query 3: diagnostic analysis with execution plan
EXPLAIN (ANALYZE, BUFFERS)
SELECT
    s.name AS service_name,
    e.method,
    e.path,
    mv.hour_bucket,
    mv.total_requests,
    mv.p95_latency_ms
FROM mv_logs_hourly_endpoint mv
JOIN endpoints e ON e.id = mv.endpoint_id
JOIN services s ON s.id = e.service_id
WHERE mv.hour_bucket >= now() - interval '24 hours'
ORDER BY mv.hour_bucket DESC, s.name, e.method, e.path
LIMIT 200;