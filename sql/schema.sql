-- Schema for the logs analytics project

CREATE TABLE IF NOT EXISTS services (
    id SERIAL PRIMARY KEY,
    name VARCHAR(120) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS endpoints (
    id SERIAL PRIMARY KEY,
    service_id INT NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    path VARCHAR(255) NOT NULL,
    method VARCHAR(10) NOT NULL,
    CONSTRAINT unique_endpoint UNIQUE (service_id, path, method)
);

CREATE TABLE IF NOT EXISTS logs_access (
    id BIGSERIAL NOT NULL,
    "timestamp" TIMESTAMP NOT NULL,
    endpoint_id INT NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    latency_ms INT NOT NULL CHECK (latency_ms >= 0),
    status_code INT NOT NULL CHECK (status_code BETWEEN 100 AND 599),
    client_ip VARCHAR(45) NOT NULL,
    PRIMARY KEY (id, "timestamp")
);

CREATE TABLE IF NOT EXISTS logs_error (
    log_id BIGINT NOT NULL,
    "timestamp" TIMESTAMP NOT NULL,
    error_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL CHECK (severity IN ('warning', 'critical')),
    message TEXT NOT NULL,
    PRIMARY KEY (log_id, "timestamp"),
    CONSTRAINT fk_logs_error_access
        FOREIGN KEY (log_id, "timestamp")
        REFERENCES logs_access (id, "timestamp")
        ON DELETE CASCADE
);