from __future__ import annotations

import logging
import os
import random
import sys
from time import perf_counter

import psycopg2
from faker import Faker
from psycopg2.extras import execute_values


DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "logs_monitoring_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres123")
CONNECT_TIMEOUT_SECONDS = int(os.getenv("DB_CONNECT_TIMEOUT", "5"))

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
TOTAL_BATCHES = int(os.getenv("TOTAL_BATCHES", "1500"))
ERROR_BATCH_SIZE = int(os.getenv("ERROR_BATCH_SIZE", "5000"))
LOOKBACK_MINUTES = int(os.getenv("LOOKBACK_MINUTES", "1440"))

STATUS_CHOICES = [200, 400, 404, 500, 503]
STATUS_WEIGHTS = [85, 5, 5, 3, 2]
ERROR_TYPES = ["timeout", "db_error", "internal"]
SEVERITIES = ["warning", "critical"]

fake = Faker("en_US")
logger = logging.getLogger("logs_generator")


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def connect_database() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connect_timeout=CONNECT_TIMEOUT_SECONDS,
    )


def load_endpoints(cursor) -> list[int]:
    cursor.execute("SELECT id FROM endpoints ORDER BY id;")
    endpoints = [row[0] for row in cursor.fetchall()]

    if not endpoints:
        raise RuntimeError("No endpoints were found in the database.")

    return endpoints


def generate_access_record(endpoint_ids: list[int]) -> tuple[int, object, int, int, str]:
    endpoint_id = random.choice(endpoint_ids)
    timestamp = fake.date_time_between(start_date=f"-{LOOKBACK_MINUTES}m", end_date="now")
    status_code = random.choices(STATUS_CHOICES, weights=STATUS_WEIGHTS, k=1)[0]
    latency_ms = random.randint(50, 200)

    if status_code >= 500:
        latency_ms = random.randint(200, 1000)

    return endpoint_id, timestamp, latency_ms, status_code, fake.ipv4()


def build_access_batch(batch_size: int, endpoint_ids: list[int]) -> list[tuple[int, object, int, int, str]]:
    return [generate_access_record(endpoint_ids) for _ in range(batch_size)]


def insert_access_batch(cursor, access_batch: list[tuple[int, object, int, int, str]]) -> None:
    execute_values(
        cursor,
        """
        INSERT INTO logs_access (endpoint_id, timestamp, latency_ms, status_code, client_ip)
        VALUES %s
        """,
        access_batch,
    )


def generate_error_record(log_id: int, timestamp) -> tuple[int, object, str, str, str]:
    return (
        log_id,
        timestamp,
        random.choice(ERROR_TYPES),
        random.choice(SEVERITIES),
        fake.sentence(),
    )


def insert_error_batches(cursor, connection) -> int:
    cursor.execute(
        """
        SELECT id, timestamp
        FROM logs_access
        WHERE status_code >= 500
        ORDER BY timestamp
        """
    )

    total_error_rows = 0

    while True:
        rows = cursor.fetchmany(ERROR_BATCH_SIZE)
        if not rows:
            break

        error_batch = [generate_error_record(log_id, timestamp) for log_id, timestamp in rows]

        execute_values(
            cursor,
            """
            INSERT INTO logs_error (log_id, timestamp, error_type, severity, message)
            VALUES %s
            ON CONFLICT (log_id, timestamp) DO NOTHING
            """,
            error_batch,
        )
        connection.commit()
        total_error_rows += len(error_batch)

    return total_error_rows


def main() -> None:
    configure_logging()
    started_at = perf_counter()

    logger.info("Starting log generator")
    logger.info("Target volume: %s rows (%s batches of %s)", TOTAL_BATCHES * BATCH_SIZE, TOTAL_BATCHES, BATCH_SIZE)

    with connect_database() as connection:
        with connection.cursor() as cursor:
            endpoint_ids = load_endpoints(cursor)
            logger.info("Loaded %s endpoints", len(endpoint_ids))

            for batch_number in range(1, TOTAL_BATCHES + 1):
                batch_started_at = perf_counter()
                access_batch = build_access_batch(BATCH_SIZE, endpoint_ids)
                insert_access_batch(cursor, access_batch)
                connection.commit()
                logger.info(
                    "Access batch %s/%s inserted in %.2fs",
                    batch_number,
                    TOTAL_BATCHES,
                    perf_counter() - batch_started_at,
                )

            logger.info("Collecting error rows")
            total_error_rows = insert_error_batches(cursor, connection)
            logger.info("Error rows inserted: %s", total_error_rows)

    logger.info("Generation completed in %.2fs", perf_counter() - started_at)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Generator failed")
        sys.exit(1)