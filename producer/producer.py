"""
Producer Vélib' → Kafka
=======================
Rôle : interroger l'API open data Vélib' Métropole à intervalle régulier
       et publier chaque enregistrement de station comme message JSON dans
       le topic Kafka `velib-stations`.

Décisions d'architecture :
- Un message = une station = un enregistrement horodaté
- La clé du message Kafka est l'identifiant de la station (station_code)
  → garantit que tous les messages d'une même station vont dans la même
    partition (ordonnancement local)
- Fréquence de collecte : toutes les 60 secondes (configurable via
  COLLECT_INTERVAL_SECONDS)
- Gestion des erreurs : retry avec backoff exponentiel si l'API est
  indisponible ; pas de perte silencieuse
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ── Configuration ────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "velib-stations")
VELIB_API_URL = os.getenv(
    "VELIB_API_URL",
    "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/"
    "velib-disponibilite-en-temps-reel/records",
)
COLLECT_INTERVAL_SECONDS = int(os.getenv("COLLECT_INTERVAL_SECONDS", "60"))
API_PAGE_SIZE = 100  # Nombre de stations par appel (max autorisé : 100)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ── Kafka Producer ────────────────────────────────────────────────────────────

def create_producer(retries: int = 10, delay: int = 5) -> KafkaProducer:
    """Crée et retourne un KafkaProducer avec retry au démarrage."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                # Sérialisation JSON avec horodatage UTC
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                # Acquittement par le leader uniquement (compromis perf/durabilité)
                acks="all",
                # Compression pour réduire la bande passante
                compression_type="gzip",
                # Retry intégré Kafka (erreurs réseau transitoires)
                retries=3,
            )
            log.info("Connecté à Kafka : %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except KafkaError as exc:
            log.warning("Tentative %d/%d – Kafka non disponible : %s", attempt, retries, exc)
            time.sleep(delay)
    raise RuntimeError("Impossible de se connecter à Kafka après %d tentatives" % retries)


# ── Ingestion API Vélib' ──────────────────────────────────────────────────────

def fetch_all_stations() -> list[dict]:
    """
    Récupère l'ensemble des stations depuis l'API Vélib'.
    L'API pagine les résultats (max 100 par page) → on itère sur toutes les pages.
    Retourne une liste de dicts bruts normalisés.
    """
    stations = []
    offset = 0
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    while True:
        params = {
            "limit": API_PAGE_SIZE,
            "offset": offset,
            "timezone": "Europe/Paris",
        }
        try:
            resp = session.get(VELIB_API_URL, params=params, timeout=15)
            resp.raise_for_status()
        except requests.exceptions.Timeout:
            log.error("Timeout lors de l'appel API (offset=%d)", offset)
            break
        except requests.exceptions.HTTPError as exc:
            log.error("Erreur HTTP API : %s", exc)
            break
        except requests.exceptions.ConnectionError as exc:
            log.error("Erreur réseau API : %s", exc)
            break

        data = resp.json()
        records = data.get("results", [])
        if not records:
            break

        stations.extend(records)
        offset += len(records)

        # Si on a récupéré moins que la taille de page, c'est la dernière page
        if len(records) < API_PAGE_SIZE:
            break

    log.info("Stations récupérées : %d", len(stations))
    return stations


def normalize_record(raw: dict, collected_at: str) -> dict:
    """
    Normalise un enregistrement brut de l'API en un message structuré.
    Ajoute un horodatage de collecte (côté producer) pour traçabilité.
    """
    return {
        # Identifiants
        "station_code":        raw.get("stationcode", ""),
        "station_name":        raw.get("name", ""),
        # Disponibilité
        "bikes_available":     int(raw.get("numbikesavailable", 0) or 0),
        "mechanical_bikes":    int(raw.get("mechanical", 0) or 0),
        "electric_bikes":      int(raw.get("ebike", 0) or 0),
        "docks_available":     int(raw.get("numdocksavailable", 0) or 0),
        "capacity":            int(raw.get("capacity", 0) or 0),
        # État
        "is_installed":        raw.get("is_installed", "NON") == "OUI",
        "is_renting":          raw.get("is_renting", "NON") == "OUI",
        "is_returning":        raw.get("is_returning", "NON") == "OUI",
        # Géolocalisation
        "latitude":            (raw.get("coordonnees_geo") or {}).get("lat"),
        "longitude":           (raw.get("coordonnees_geo") or {}).get("lon"),
        # Horodatages
        "last_reported":       raw.get("duedate", ""),   # horodatage source
        "collected_at":        collected_at,              # horodatage producer
    }


# ── Boucle principale ─────────────────────────────────────────────────────────

def delivery_report(err, msg):
    """Callback appelé après l'envoi d'un message Kafka."""
    if err is not None:
        log.error("Échec envoi message (station=%s) : %s", msg.key(), err)


def run():
    producer = create_producer()
    log.info(
        "Producer démarré – topic=%s, intervalle=%ds",
        KAFKA_TOPIC,
        COLLECT_INTERVAL_SECONDS,
    )

    while True:
        cycle_start = time.time()
        collected_at = datetime.now(timezone.utc).isoformat()

        stations = fetch_all_stations()
        sent = 0
        for raw in stations:
            record = normalize_record(raw, collected_at)
            station_code = record["station_code"]
            if not station_code:
                continue
            producer.send(
                KAFKA_TOPIC,
                key=station_code,
                value=record,
            )
            sent += 1

        producer.flush()
        log.info("Cycle terminé – %d messages envoyés dans le topic '%s'", sent, KAFKA_TOPIC)

        # Attente jusqu'au prochain cycle (en soustrayant le temps de collecte)
        elapsed = time.time() - cycle_start
        sleep_time = max(0, COLLECT_INTERVAL_SECONDS - elapsed)
        log.info("Prochain cycle dans %.1fs", sleep_time)
        time.sleep(sleep_time)


if __name__ == "__main__":
    run()
