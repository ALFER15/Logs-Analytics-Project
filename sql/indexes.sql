-- Indexes for analytical queries and dashboard responsiveness

CREATE INDEX IF NOT EXISTS idx_logs_access_timestamp_desc
    ON logs_access ("timestamp" DESC);

CREATE INDEX IF NOT EXISTS idx_logs_access_endpoint_timestamp
    ON logs_access (endpoint_id, "timestamp" DESC);

CREATE INDEX IF NOT EXISTS idx_logs_access_status_code
    ON logs_access (status_code);

CREATE INDEX IF NOT EXISTS idx_logs_error_timestamp_desc
    ON logs_error ("timestamp" DESC);

CREATE INDEX IF NOT EXISTS idx_logs_error_severity
    ON logs_error (severity);

CREATE INDEX IF NOT EXISTS idx_endpoints_service_path_method
    ON endpoints (service_id, path, method);