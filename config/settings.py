"""
Configuration centralisée du projet Vélib' Pipeline
Toutes les variables d'environnement avec leurs valeurs par défaut.
"""

import os

# ── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "velib-stations")
KAFKA_GROUP_ID          = os.getenv("KAFKA_GROUP_ID", "velib-dwh-consumer")
KAFKA_LOG_RETENTION_H   = int(os.getenv("KAFKA_LOG_RETENTION_HOURS", "24"))

# ── API Vélib' ────────────────────────────────────────────────────────────────
VELIB_API_URL = os.getenv(
    "VELIB_API_URL",
    "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/"
    "velib-disponibilite-en-temps-reel/records",
)
COLLECT_INTERVAL_SECONDS = int(os.getenv("COLLECT_INTERVAL_SECONDS", "60"))

# ── PostgreSQL DWH ────────────────────────────────────────────────────────────
POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", "5433"))
POSTGRES_DB       = os.getenv("POSTGRES_DB", "velib_dwh")
POSTGRES_USER     = os.getenv("POSTGRES_USER", "velib")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "velib")

POSTGRES_DSN = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# ── Airflow ───────────────────────────────────────────────────────────────────
AIRFLOW_SCHEDULE = os.getenv("AIRFLOW_SCHEDULE", "*/5 * * * *")  # toutes les 5 min
