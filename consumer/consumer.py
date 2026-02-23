from __future__ import annotations  # Python 3.8 compat (Airflow 2.8.1)

"""
Consumer Kafka → PostgreSQL (Data Warehouse)
============================================
Rôle : consommer les messages du topic Kafka `velib-stations`,
       les valider, les transformer et les insérer dans le DWH PostgreSQL.

Deux couches de données :
  1. raw.station_snapshots  – données brutes horodatées (append-only)
  2. mart.station_latest    – dernière valeur connue par station (upsert)

Ce consumer est invoqué comme une tâche Airflow (KafkaConsumerOperator custom)
mais peut aussi tourner en standalone pour le développement.
"""

import json
import logging
import os
import time

import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ── Configuration ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "velib-stations")
KAFKA_GROUP_ID          = os.getenv("KAFKA_GROUP_ID", "velib-dwh-consumer")
# Lecture depuis le dernier offset non consommé (earliest = reprise depuis le début)
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

PG_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://velib:velib@localhost:5433/velib_dwh",
)

# Commit Kafka tous les N messages (performances vs durabilité)
COMMIT_EVERY_N = int(os.getenv("COMMIT_EVERY_N", "50"))
# Temps max d'attente de nouveaux messages avant de terminer (mode batch Airflow)
POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "5000"))
# Nombre max de messages traités par exécution Airflow (0 = illimité / daemon)
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "0"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ── Connexion PostgreSQL ──────────────────────────────────────────────────────

def get_pg_connection(retries: int = 10, delay: int = 3):
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(PG_DSN)
            conn.autocommit = False
            log.info("Connecté à PostgreSQL DWH")
            return conn
        except psycopg2.OperationalError as exc:
            log.warning("Tentative PG %d/%d : %s", attempt, retries, exc)
            time.sleep(delay)
    raise RuntimeError("Impossible de se connecter à PostgreSQL")


# ── Transformations / validations ─────────────────────────────────────────────

def validate_message(msg: dict) -> bool:
    """Rejette les messages sans station_code ou avec une capacité nulle/négative."""
    if not msg.get("station_code"):
        return False
    if msg.get("capacity", 0) < 0:
        return False
    return True


def compute_occupancy_rate(msg: dict) -> float | None:
    """Taux d'occupation = vélos disponibles / capacité totale."""
    capacity = msg.get("capacity", 0)
    if capacity and capacity > 0:
        return round(msg.get("bikes_available", 0) / capacity, 4)
    return None


def enrich_message(msg: dict) -> dict:
    """Ajoute des champs calculés au message avant insertion en base."""
    msg["occupancy_rate"] = compute_occupancy_rate(msg)
    # Statut synthétique de la station
    if not msg.get("is_installed"):
        msg["station_status"] = "hors_service"
    elif not msg.get("is_renting") and not msg.get("is_returning"):
        msg["station_status"] = "fermée"
    else:
        msg["station_status"] = "en_service"
    return msg


# ── Insertions DWH ────────────────────────────────────────────────────────────

INSERT_SNAPSHOT = """
    INSERT INTO raw.station_snapshots (
        station_code, station_name,
        bikes_available, mechanical_bikes, electric_bikes,
        docks_available, capacity,
        is_installed, is_renting, is_returning,
        occupancy_rate, station_status,
        latitude, longitude,
        last_reported, collected_at
    ) VALUES (
        %(station_code)s, %(station_name)s,
        %(bikes_available)s, %(mechanical_bikes)s, %(electric_bikes)s,
        %(docks_available)s, %(capacity)s,
        %(is_installed)s, %(is_renting)s, %(is_returning)s,
        %(occupancy_rate)s, %(station_status)s,
        %(latitude)s, %(longitude)s,
        %(last_reported)s, %(collected_at)s
    )
    ON CONFLICT DO NOTHING;
"""

UPSERT_LATEST = """
    INSERT INTO mart.station_latest (
        station_code, station_name,
        bikes_available, mechanical_bikes, electric_bikes,
        docks_available, capacity,
        is_installed, is_renting, is_returning,
        occupancy_rate, station_status,
        latitude, longitude,
        last_reported, collected_at
    ) VALUES (
        %(station_code)s, %(station_name)s,
        %(bikes_available)s, %(mechanical_bikes)s, %(electric_bikes)s,
        %(docks_available)s, %(capacity)s,
        %(is_installed)s, %(is_renting)s, %(is_returning)s,
        %(occupancy_rate)s, %(station_status)s,
        %(latitude)s, %(longitude)s,
        %(last_reported)s, %(collected_at)s
    )
    ON CONFLICT (station_code) DO UPDATE SET
        station_name      = EXCLUDED.station_name,
        bikes_available   = EXCLUDED.bikes_available,
        mechanical_bikes  = EXCLUDED.mechanical_bikes,
        electric_bikes    = EXCLUDED.electric_bikes,
        docks_available   = EXCLUDED.docks_available,
        capacity          = EXCLUDED.capacity,
        is_installed      = EXCLUDED.is_installed,
        is_renting        = EXCLUDED.is_renting,
        is_returning      = EXCLUDED.is_returning,
        occupancy_rate    = EXCLUDED.occupancy_rate,
        station_status    = EXCLUDED.station_status,
        latitude          = EXCLUDED.latitude,
        longitude         = EXCLUDED.longitude,
        last_reported     = EXCLUDED.last_reported,
        collected_at      = EXCLUDED.collected_at
    WHERE EXCLUDED.collected_at > mart.station_latest.collected_at;
"""


def insert_batch(cursor, batch: list[dict]):
    """Insère un lot de messages dans raw + mart."""
    psycopg2.extras.execute_batch(cursor, INSERT_SNAPSHOT, batch, page_size=100)
    psycopg2.extras.execute_batch(cursor, UPSERT_LATEST, batch, page_size=100)
    log.info("Batch de %d enregistrements inséré", len(batch))


# ── Boucle de consommation ────────────────────────────────────────────────────

def run():
    conn = get_pg_connection()
    cursor = conn.cursor()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
        enable_auto_commit=False,   # commit manuel pour éviter les pertes
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=POLL_TIMEOUT_MS,
    )
    log.info("Consumer démarré – topic=%s, group=%s", KAFKA_TOPIC, KAFKA_GROUP_ID)

    batch = []
    total_processed = 0
    total_rejected  = 0

    try:
        for kafka_msg in consumer:
            msg = kafka_msg.value
            if not validate_message(msg):
                total_rejected += 1
                continue

            msg = enrich_message(msg)
            batch.append(msg)

            if len(batch) >= COMMIT_EVERY_N:
                insert_batch(cursor, batch)
                conn.commit()
                consumer.commit()
                total_processed += len(batch)
                batch = []

            if MAX_MESSAGES > 0 and total_processed >= MAX_MESSAGES:
                log.info("Limite MAX_MESSAGES=%d atteinte", MAX_MESSAGES)
                break

    except KafkaError as exc:
        log.error("Erreur Kafka : %s", exc)
        conn.rollback()
    finally:
        # Flush du dernier batch partiel
        if batch:
            insert_batch(cursor, batch)
            conn.commit()
            consumer.commit()
            total_processed += len(batch)

        consumer.close()
        cursor.close()
        conn.close()
        log.info(
            "Consumer terminé – traités : %d, rejetés : %d",
            total_processed,
            total_rejected,
        )


if __name__ == "__main__":
    run()
