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
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.exceptions import AirflowSkipException
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
CONSUMER_SCRIPT = "/opt/airflow/consumer/consumer.py"


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
        log.info("Kafka OK – topics disponibles : %s", topics)
    except KafkaError as exc:
        raise RuntimeError("Kafka inaccessible : %s" % exc) from exc


def check_dwh_health(**ctx):
    """Vérifie la connectivité PostgreSQL DWH."""
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


def consume_kafka_batch(**ctx):
    """
    Invoque le consumer Python en sous-processus.
    Paramètres passés via variables d'environnement :
      - MAX_MESSAGES : nombre max de messages à consommer par exécution
      - POLL_TIMEOUT_MS : délai d'attente si le topic est vide
    Ce mécanisme permet de transformer le consumer daemon en tâche Airflow bornée.
    """
    env = os.environ.copy()
    env.update({
        "POSTGRES_DSN":           PG_DSN,
        "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP,
        "KAFKA_TOPIC":            KAFKA_TOPIC,
        "KAFKA_GROUP_ID":         "velib-airflow-consumer",
        "MAX_MESSAGES":           "5000",   # ~5 000 messages par cycle
        "POLL_TIMEOUT_MS":        "10000",  # 10s sans message → fin du batch
        "COMMIT_EVERY_N":         "100",
    })

    result = subprocess.run(
        [sys.executable, CONSUMER_SCRIPT],
        env=env,
        capture_output=True,
        text=True,
        timeout=300,  # 5 min max
    )
    if result.returncode != 0:
        log.error("Consumer stderr : %s", result.stderr)
        raise RuntimeError("Consumer terminé avec code %d" % result.returncode)
    log.info("Consumer stdout : %s", result.stdout[-2000:])


def refresh_hourly_agg(**ctx):
    """Rafraîchit la vue matérialisée des agrégats horaires."""
    _run_sql("REFRESH MATERIALIZED VIEW CONCURRENTLY mart.hourly_station_stats;")


def refresh_daily_agg(**ctx):
    """Rafraîchit la vue matérialisée des agrégats journaliers."""
    _run_sql("REFRESH MATERIALIZED VIEW CONCURRENTLY mart.daily_station_stats;")


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
            -- Minutes en saturation (0 borne libre)
            COUNT(*) FILTER (WHERE s.docks_available = 0) * 1      AS saturation_minutes,
            -- Minutes à vide (0 vélo dispo)
            COUNT(*) FILTER (WHERE s.bikes_available = 0) * 1      AS empty_minutes,
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
      - Pas de snapshots avec capacité = 0
      - Cohérence : bikes_available <= capacity
      - Fraîcheur : au moins 1 snapshot dans les 10 dernières minutes
    """
    checks = {
        "capacity_zero": """
            SELECT COUNT(*) FROM raw.station_snapshots
            WHERE capacity = 0 AND collected_at >= NOW() - INTERVAL '10 minutes';
        """,
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
    cur = conn.cursor()
    issues = []

    for check_name, sql in checks.items():
        cur.execute(sql)
        count = cur.fetchone()[0]
        if check_name == "recent_data_exists" and count == 0:
            issues.append("ALERTE: Aucune donnée récente (< 10 min) dans raw.station_snapshots")
        elif check_name != "recent_data_exists" and count > 0:
            issues.append("ALERTE [%s]: %d enregistrements problématiques" % (check_name, count))
        else:
            log.info("Check '%s' : OK (count=%d)", check_name, count)

    cur.close()
    conn.close()

    if issues:
        for issue in issues:
            log.warning(issue)
        # On ne fait pas échouer le DAG sur des alertes qualité (avertissement uniquement)
        # Pour rendre bloquant : raise AirflowException("\n".join(issues))
    else:
        log.info("Tous les contrôles qualité sont OK")


def _run_sql(sql: str):
    """Exécute une requête SQL sur le DWH."""
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
    schedule_interval="*/5 * * * *",   # Toutes les 5 minutes
    catchup=False,
    max_active_runs=1,                  # Pas d'exécutions parallèles du même DAG
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

    # ── Dépendances ────────────────────────────────────────────────────────────
    #
    #  check_kafka ──┐
    #                ├─► consume ─► hourly_agg ─► daily_agg ─► rank
    #  check_dwh  ──┘                                        └─► quality
    #
    [t_check_kafka, t_check_dwh] >> t_consume
    t_consume >> t_hourly >> t_daily
    t_daily >> [t_rank, t_quality]
