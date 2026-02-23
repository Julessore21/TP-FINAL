"""
DAG Airflow – Pipeline Vélib' Métropole
========================================
Planification : toutes les 5 minutes
Chaîne de traitement :
  1. check_kafka_health    – vérifie que Kafka est accessible
  2. check_dwh_health      – vérifie que le DWH PostgreSQL est accessible
  3. consume_kafka_batch   – consomme le topic Kafka et charge le DWH
  4. refresh_hourly_agg    – agrège les données brutes en granularité horaire
  5. refresh_daily_agg     – agrège en granularité journalière
  6. compute_station_rank  – calcule le ranking des stations (saturées / vides)
  7. data_quality_check    – contrôles qualité sur les données insérées

Dépendances :
  check_kafka_health  ──┐
                        ├─► consume_kafka_batch ─► refresh_hourly_agg ─► refresh_daily_agg
  check_dwh_health   ──┘                                                        │
                                                                                 └─► compute_station_rank
                                                                                 └─► data_quality_check

Note d'architecture :
  Tous les imports tiers (kafka, psycopg2) sont DANS les fonctions (lazy imports).
  Cela garantit que le DAG se charge correctement même si les packages
  ne sont pas encore installés, et évite tout problème de Python 3.8 compat.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

# ── Paramètres du DAG ─────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":            "data-team",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=1),
    "email_on_failure": False,
    "email_on_retry":   False,
}

PG_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://velib:velib@postgres-dwh:5432/velib_dwh",
)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC     = "velib-stations"

# ── SQL d'insertion ───────────────────────────────────────────────────────────

_INSERT_SNAPSHOT = """
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
    ON CONFLICT (station_code, collected_at) DO NOTHING;
"""

_UPSERT_LATEST = """
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


# ── Fonctions des tâches ──────────────────────────────────────────────────────

def check_kafka_health(**ctx):
    """Vérifie la connectivité Kafka via kafka-python."""
    from kafka import KafkaAdminClient
    from kafka.errors import KafkaError
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            request_timeout_ms=5000,
        )
        topics = admin.list_topics()
        admin.close()
        log.info("Kafka OK – topics : %s", topics)
    except KafkaError as exc:
        raise RuntimeError("Kafka inaccessible : %s" % exc) from exc


def check_dwh_health(**ctx):
    """Vérifie la connectivité PostgreSQL DWH."""
    import psycopg2
    try:
        conn = psycopg2.connect(PG_DSN, connect_timeout=5)
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        cur.close()
        conn.close()
        log.info("PostgreSQL DWH OK – %s", version)
    except psycopg2.Error as exc:
        raise RuntimeError("DWH inaccessible : %s" % exc) from exc


def _enrich(record):
    """Ajoute occupancy_rate et station_status à un enregistrement."""
    capacity = record.get("capacity") or 0
    if capacity > 0:
        record["occupancy_rate"] = round(
            (record.get("bikes_available") or 0) / capacity, 4
        )
    else:
        record["occupancy_rate"] = None

    if not record.get("is_installed"):
        record["station_status"] = "hors_service"
    elif not record.get("is_renting") and not record.get("is_returning"):
        record["station_status"] = "fermee"
    else:
        record["station_status"] = "en_service"
    return record


def consume_kafka_batch(**ctx):
    """
    Consomme un batch depuis le topic Kafka `velib-stations` et insère
    les données dans raw.station_snapshots et mart.station_latest.

    Architecture : logique inlinée dans le DAG (pas de subprocess).
    Tous les imports tiers sont lazys pour garantir le chargement du DAG.

    Paramètres :
      - MAX_MESSAGES  : 5 000 messages max par cycle (configurable)
      - POLL_TIMEOUT_MS : 10s sans message → fin du batch
    """
    import json
    import psycopg2
    import psycopg2.extras
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError

    MAX_MESSAGES    = 5000
    POLL_TIMEOUT_MS = 10000   # 10s sans message → fin du batch
    BATCH_SIZE      = 50      # Commit toutes les 50 lignes

    # ── Connexion PostgreSQL ──────────────────────────────────────────────────
    conn = psycopg2.connect(PG_DSN, connect_timeout=10)
    conn.autocommit = False
    cursor = conn.cursor()
    log.info("Connecté au DWH : %s", PG_DSN)

    # ── Consumer Kafka ────────────────────────────────────────────────────────
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="velib-airflow-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=POLL_TIMEOUT_MS,
    )
    log.info("Consumer Kafka connecté – topic=%s, bootstrap=%s",
             KAFKA_TOPIC, KAFKA_BOOTSTRAP)

    batch     = []
    total     = 0
    rejected  = 0

    try:
        for kafka_msg in consumer:
            record = kafka_msg.value

            # Validation minimale
            if not record.get("station_code"):
                rejected += 1
                continue

            record = _enrich(record)
            batch.append(record)

            # Commit par batch
            if len(batch) >= BATCH_SIZE:
                psycopg2.extras.execute_batch(cursor, _INSERT_SNAPSHOT, batch)
                psycopg2.extras.execute_batch(cursor, _UPSERT_LATEST,   batch)
                conn.commit()
                consumer.commit()
                total += len(batch)
                log.info("Batch de %d enregistrements inséré (total=%d)", len(batch), total)
                batch = []

            if MAX_MESSAGES > 0 and total >= MAX_MESSAGES:
                log.info("MAX_MESSAGES=%d atteint", MAX_MESSAGES)
                break

    except KafkaError as exc:
        conn.rollback()
        raise RuntimeError("Erreur Kafka pendant la consommation : %s" % exc) from exc

    finally:
        # Flush du dernier batch partiel
        if batch:
            psycopg2.extras.execute_batch(cursor, _INSERT_SNAPSHOT, batch)
            psycopg2.extras.execute_batch(cursor, _UPSERT_LATEST,   batch)
            conn.commit()
            consumer.commit()
            total += len(batch)

        consumer.close()
        cursor.close()
        conn.close()

    log.info("consume_kafka_batch terminé – insérés : %d, rejetés : %d", total, rejected)
    return {"inserted": total, "rejected": rejected}


def refresh_hourly_agg(**ctx):
    """Rafraîchit la vue matérialisée des agrégats horaires."""
    _run_sql("REFRESH MATERIALIZED VIEW CONCURRENTLY mart.hourly_station_stats;")
    log.info("Vue mart.hourly_station_stats rafraîchie")


def refresh_daily_agg(**ctx):
    """Rafraîchit la vue matérialisée des agrégats journaliers."""
    _run_sql("REFRESH MATERIALIZED VIEW CONCURRENTLY mart.daily_station_stats;")
    log.info("Vue mart.daily_station_stats rafraîchie")


def compute_station_rank(**ctx):
    """
    Calcule et insère dans mart.station_ranking le classement des stations
    selon leur taux de saturation moyen sur les dernières 24h.
    """
    sql = """
        INSERT INTO mart.station_ranking (
            computed_at,
            station_code,
            station_name,
            avg_occupancy_rate,
            avg_bikes_available,
            saturation_minutes,
            empty_minutes,
            rank_saturation,
            rank_availability
        )
        SELECT
            NOW()                                                   AS computed_at,
            s.station_code,
            s.station_name,
            ROUND(AVG(s.occupancy_rate)::numeric, 4)               AS avg_occupancy_rate,
            ROUND(AVG(s.bikes_available)::numeric, 2)              AS avg_bikes_available,
            COUNT(*) FILTER (WHERE s.docks_available = 0)          AS saturation_minutes,
            COUNT(*) FILTER (WHERE s.bikes_available = 0)          AS empty_minutes,
            RANK() OVER (ORDER BY AVG(s.occupancy_rate) DESC)      AS rank_saturation,
            RANK() OVER (ORDER BY AVG(s.bikes_available) ASC)      AS rank_availability
        FROM raw.station_snapshots s
        WHERE s.collected_at >= NOW() - INTERVAL '24 hours'
          AND s.occupancy_rate IS NOT NULL
        GROUP BY s.station_code, s.station_name
        ON CONFLICT (computed_at, station_code) DO NOTHING;
    """
    _run_sql(sql)
    log.info("Ranking des stations calculé")


def data_quality_check(**ctx):
    """
    Contrôles qualité sur les données insérées dans le cycle courant.
    Vérifie :
      - Cohérence : bikes_available <= capacity
      - Fraîcheur : au moins 1 snapshot dans les 10 dernières minutes
    """
    import psycopg2
    checks = {
        "bikes_exceed_capacity": """
            SELECT COUNT(*) FROM raw.station_snapshots
            WHERE bikes_available > capacity
              AND collected_at >= NOW() - INTERVAL '10 minutes';
        """,
        "recent_data_exists": """
            SELECT COUNT(*) FROM raw.station_snapshots
            WHERE collected_at >= NOW() - INTERVAL '10 minutes';
        """,
    }

    conn = psycopg2.connect(PG_DSN)
    cur  = conn.cursor()

    for check_name, sql in checks.items():
        cur.execute(sql)
        count = cur.fetchone()[0]
        if check_name == "recent_data_exists" and count == 0:
            log.warning("ALERTE: aucune donnée récente (< 10 min)")
        elif check_name != "recent_data_exists" and count > 0:
            log.warning("ALERTE [%s]: %d enregistrements problématiques", check_name, count)
        else:
            log.info("Check '%s' : OK (count=%d)", check_name, count)

    cur.close()
    conn.close()


def _run_sql(sql):
    """Exécute une requête SQL sur le DWH (psycopg2 lazy import)."""
    import psycopg2
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(sql)
    finally:
        cur.close()
        conn.close()


# ── Définition du DAG ─────────────────────────────────────────────────────────

with DAG(
    dag_id="velib_pipeline",
    description="Pipeline temps réel Vélib' Métropole – Kafka → PostgreSQL DWH",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["velib", "streaming", "dwh"],
) as dag:

    t_check_kafka = PythonOperator(
        task_id="check_kafka_health",
        python_callable=check_kafka_health,
        doc_md="Vérifie que le broker Kafka est accessible avant de consommer.",
    )

    t_check_dwh = PythonOperator(
        task_id="check_dwh_health",
        python_callable=check_dwh_health,
        doc_md="Vérifie que le DWH PostgreSQL est accessible avant d'écrire.",
    )

    t_consume = PythonOperator(
        task_id="consume_kafka_batch",
        python_callable=consume_kafka_batch,
        execution_timeout=timedelta(minutes=4),
        doc_md=(
            "Consomme un batch depuis le topic `velib-stations` et insère "
            "les données dans raw.station_snapshots et mart.station_latest."
        ),
    )

    t_hourly = PythonOperator(
        task_id="refresh_hourly_agg",
        python_callable=refresh_hourly_agg,
        doc_md="Rafraîchit les agrégats horaires (MATERIALIZED VIEW).",
    )

    t_daily = PythonOperator(
        task_id="refresh_daily_agg",
        python_callable=refresh_daily_agg,
        doc_md="Rafraîchit les agrégats journaliers (MATERIALIZED VIEW).",
    )

    t_rank = PythonOperator(
        task_id="compute_station_rank",
        python_callable=compute_station_rank,
        doc_md="Calcule le ranking des stations les plus saturées / les plus vides.",
    )

    t_quality = PythonOperator(
        task_id="data_quality_check",
        python_callable=data_quality_check,
        doc_md="Contrôles qualité : cohérence des données, fraîcheur.",
    )

    # ── Graphe de dépendances ─────────────────────────────────────────────────
    #
    #  check_kafka ──┐
    #                ├──► consume ──► hourly_agg ──► daily_agg ──► rank
    #  check_dwh  ──┘                                           └──► quality
    #
    [t_check_kafka, t_check_dwh] >> t_consume
    t_consume >> t_hourly >> t_daily
    t_daily >> [t_rank, t_quality]
