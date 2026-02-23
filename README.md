# Pipeline de données temps réel – Vélib' Métropole

## Table des matières
1. [Architecture globale](#1-architecture-globale)
2. [Flux de données](#2-flux-de-données)
3. [Composants techniques](#3-composants-techniques)
4. [Modèle de données](#4-modèle-de-données)
5. [Orchestration Airflow](#5-orchestration-airflow)
6. [Indicateurs analytiques](#6-indicateurs-analytiques)
7. [Démarrage rapide](#7-démarrage-rapide)
8. [Justifications des choix](#8-justifications-des-choix)

---

## 1. Architecture globale

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Pipeline Vélib' Métropole                        │
│                                                                         │
│  ┌──────────────┐    ┌───────────────────────────────┐                 │
│  │  API Vélib'  │───►│         KAFKA BROKER          │                 │
│  │  (open data) │    │   Topic: velib-stations        │                 │
│  └──────────────┘    │   Partitionnement: station_code│                 │
│       ▲              │   Rétention: 24h               │                 │
│       │ 60s          └───────────────┬───────────────┘                 │
│  ┌────┴─────────┐                   │                                  │
│  │   PRODUCER   │             ┌─────▼──────────────────────────────┐  │
│  │  (Python)    │             │           AIRFLOW DAG               │  │
│  └──────────────┘             │  (planifié toutes les 5 minutes)    │  │
│                               │                                     │  │
│                               │  1. check_kafka_health              │  │
│                               │  2. check_dwh_health                │  │
│                               │  3. consume_kafka_batch             │  │
│                               │  4. refresh_hourly_agg              │  │
│                               │  5. refresh_daily_agg               │  │
│                               │  6. compute_station_rank            │  │
│                               │  7. data_quality_check              │  │
│                               └─────────────┬──────────────────────┘  │
│                                             │                          │
│                               ┌─────────────▼──────────────────────┐  │
│                               │      POSTGRESQL (DWH)               │  │
│                               │  raw.station_snapshots              │  │
│                               │  mart.station_latest                │  │
│                               │  mart.hourly_station_stats (MV)     │  │
│                               │  mart.daily_station_stats (MV)      │  │
│                               │  mart.station_ranking               │  │
│                               └─────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Flux de données

### 2.1 Ingestion (Producer → Kafka)

```
API Vélib' (HTTPS)
      │  GET /records?limit=100&offset=N
      │  (pagination complète, ~1 500 stations)
      ▼
  producer.py
      │  normalize_record()     → enrichissement minimal (horodatage)
      │  KafkaProducer.send()   → clé = station_code
      ▼
Topic Kafka: velib-stations
      │  1 message = 1 station = 1 snapshot JSON
      │  Fréquence : 1 collecte / 60 secondes
      │  ~1 500 messages par cycle
```

**Format d'un message Kafka :**
```json
{
  "station_code":     "16107",
  "station_name":     "Benjamin Godard - Victor Hugo",
  "bikes_available":  8,
  "mechanical_bikes": 5,
  "electric_bikes":   3,
  "docks_available":  10,
  "capacity":         18,
  "is_installed":     true,
  "is_renting":       true,
  "is_returning":     true,
  "latitude":         48.865983,
  "longitude":        2.275725,
  "last_reported":    "2024-01-15T08:32:00+01:00",
  "collected_at":     "2024-01-15T07:32:05.123Z"
}
```

### 2.2 Traitement (Kafka → DWH)

```
Topic Kafka: velib-stations
      │
      ▼
  consumer.py (invoqué par Airflow)
      │  validate_message()   → rejet des messages invalides
      │  enrich_message()     → calcul occupancy_rate, station_status
      │  insert_batch()       → INSERT raw + UPSERT mart
      ▼
PostgreSQL DWH
      ├── raw.station_snapshots  (append-only)
      └── mart.station_latest    (upsert – dernière valeur)
```

### 2.3 Agrégation (Airflow → vues matérialisées)

```
raw.station_snapshots
      │
      ├── REFRESH MATERIALIZED VIEW mart.hourly_station_stats
      ├── REFRESH MATERIALIZED VIEW mart.daily_station_stats
      └── INSERT mart.station_ranking  (ranking calculé en SQL)
```

---

## 3. Composants techniques

| Composant | Image Docker | Port | Rôle |
|-----------|-------------|------|------|
| Zookeeper | `confluentinc/cp-zookeeper:7.6.0` | 2181 | Coordination Kafka |
| Kafka | `confluentinc/cp-kafka:7.6.0` | 9092 | Broker de streaming |
| Kafka UI | `provectuslabs/kafka-ui` | 8081 | Monitoring Kafka |
| PostgreSQL (Airflow) | `postgres:15` | 5432 (interne) | Métadonnées Airflow |
| PostgreSQL (DWH) | `postgres:15` | 5433 | Entrepôt analytique |
| Producer | Custom Python | — | Ingestion API Vélib' |
| Airflow Webserver | `apache/airflow:2.8.1` | 8080 | Interface DAGs |
| Airflow Scheduler | `apache/airflow:2.8.1` | — | Planification |

---

## 4. Modèle de données

### Choix de modélisation

Le modèle adopte une **architecture médaillon simplifiée** :
- **Couche RAW** : données brutes, append-only, immuables
- **Couche MART** : données transformées et agrégées pour l'analyse

**Pourquoi PostgreSQL et non ClickHouse ?**
- Volume modéré : ~1 500 stations × 1 msg/min = ~2 160 000 lignes/jour
- SQL analytique classique suffisant pour les requêtes ciblées
- Vues matérialisées PostgreSQL offrent de bonnes performances
- Simplicité de déploiement et de maintenance

### Schéma ERD simplifié

```
raw.station_snapshots (append-only)
├── snapshot_id       BIGSERIAL PK
├── station_code      VARCHAR(10)   ← clé naturelle
├── station_name      VARCHAR(200)
├── bikes_available   SMALLINT
├── mechanical_bikes  SMALLINT
├── electric_bikes    SMALLINT
├── docks_available   SMALLINT
├── capacity          SMALLINT
├── is_installed      BOOLEAN
├── is_renting        BOOLEAN
├── is_returning      BOOLEAN
├── occupancy_rate    NUMERIC(5,4)  ← calculé : bikes/capacity
├── station_status    VARCHAR(20)   ← 'en_service'|'fermée'|'hors_service'
├── latitude          DOUBLE
├── longitude         DOUBLE
├── last_reported     TIMESTAMPTZ   ← horodatage source API
└── collected_at      TIMESTAMPTZ   ← horodatage producer

mart.station_latest (upsert)
└── Dernière valeur connue par station (PK = station_code)

mart.hourly_station_stats (MATERIALIZED VIEW)
└── Agrégats horaires : AVG, MIN, MAX, COUNT saturé/vide

mart.daily_station_stats (MATERIALIZED VIEW)
└── Agrégats journaliers : même métriques que horaire

mart.station_ranking (table)
└── Classement des stations saturées/vides calculé toutes les 5 min
```

---

## 5. Orchestration Airflow

### DAG : `velib_pipeline`

- **Planification** : `*/5 * * * *` (toutes les 5 minutes)
- **max_active_runs** : 1 (pas d'exécutions parallèles)
- **Retry** : 2 tentatives avec 1 minute de délai

### Graphe de dépendances

```
check_kafka_health ──┐
                     ├──► consume_kafka_batch ──► refresh_hourly_agg ──► refresh_daily_agg ──► compute_station_rank
check_dwh_health  ──┘                                                                        └──► data_quality_check
```

### Description des tâches

| Tâche | Durée max | Description |
|-------|-----------|-------------|
| `check_kafka_health` | 30s | Vérifie la connectivité Kafka avant tout traitement |
| `check_dwh_health` | 30s | Vérifie la connectivité PostgreSQL |
| `consume_kafka_batch` | 4 min | Consomme jusqu'à 5 000 messages Kafka, insère dans raw + mart |
| `refresh_hourly_agg` | 60s | Rafraîchit la vue matérialisée horaire |
| `refresh_daily_agg` | 60s | Rafraîchit la vue matérialisée journalière |
| `compute_station_rank` | 60s | Calcule le ranking des stations sur 24h glissantes |
| `data_quality_check` | 30s | Contrôles qualité : cohérence, fraîcheur des données |

### Gestion des échecs

- Les tâches `check_*` bloquent l'exécution si les services sont inaccessibles
- La tâche `data_quality_check` émet des avertissements sans bloquer le DAG
- Le consumer est conçu pour être **idempotent** : un re-run ne duplique pas les données (contrainte `UNIQUE` sur `station_code, collected_at`)

---

## 6. Indicateurs analytiques

### Indicateur 1 – Disponibilité globale en temps réel
Agrège l'état de toutes les stations actives pour donner une vue macro du réseau.

**Métriques clés :**
- Nombre total de vélos disponibles (mécaniques + électriques)
- Nombre de stations vides / saturées
- Taux d'occupation moyen du réseau

### Indicateur 2 – Stations les plus saturées (24h)
Classe les stations selon leur **taux d'occupation moyen** et leur **durée en saturation** sur les dernières 24h.

**Utilité :** identifier les zones à forte demande pour optimiser la redistribution.

### Indicateur 3 – Stations les plus souvent vides (24h)
Identifie les stations chroniquement sous-approvisionnées.

**Utilité :** signaler les points noirs du réseau aux opérateurs.

### Indicateur 4 – Variation temporelle sur 24h (par heure)
Courbe d'utilisation du réseau heure par heure : taux d'occupation, vélos disponibles.

**Utilité :** visualiser les pics d'usage (rush matin/soir), adapter les rondes de réappro.

### Indicateur 5 – Vélos mécaniques vs électriques
Répartition et évolution temporelle de la disponibilité par type de vélo.

**Utilité :** mesurer la préférence des usagers pour les vélos électriques, ajuster les parcs.

### Indicateur 6 – Stations problématiques (7 jours)
Détecte les stations fréquemment hors service ou avec des données incohérentes.

**Utilité :** maintenance préventive, détection d'anomalies.

### Indicateur 7 – Tendance journalière sur 7 jours
Vue macro de l'évolution du réseau jour après jour.

**Utilité :** détecter des tendances de fond (weekend vs semaine, météo).

### Indicateur 8 – Heatmap heures × jours de la semaine
Matrice croisant le jour de la semaine et l'heure pour révéler des patterns récurrents.

**Utilité :** identifier les comportements cycliques (lundi 8h = pic, dimanche 14h = creux).

---

## 7. Démarrage rapide

### Prérequis
- Docker Desktop ≥ 4.x
- Docker Compose v2

### Lancement

```bash
# 1. Démarrer tous les services
docker compose up -d

# 2. Attendre l'initialisation d'Airflow (~2 minutes)
docker compose logs -f airflow-init

# 3. Vérifier que tous les services sont UP
docker compose ps
```

### Accès aux interfaces

| Service | URL | Identifiants |
|---------|-----|--------------|
| Airflow | http://localhost:8080 | admin / admin |
| Kafka UI | http://localhost:8081 | — |
| PostgreSQL DWH | localhost:5433 | velib / velib |

### Connexion au DWH

```bash
# Depuis l'hôte
psql -h localhost -p 5433 -U velib -d velib_dwh

# Depuis un container
docker exec -it postgres-dwh psql -U velib -d velib_dwh
```

### Exécution des indicateurs

```bash
docker exec -it postgres-dwh psql -U velib -d velib_dwh \
  -f /docker-entrypoint-initdb.d/indicators.sql
```

---

## 8. Justifications des choix

### Kafka comme système de streaming

Kafka découple **l'ingestion** (producer) du **traitement** (consumer/Airflow).
Ce découplage apporte :
- **Résilience** : si le DWH est temporairement indisponible, les messages restent dans le topic (24h de rétention)
- **Replay** : possibilité de re-traiter les données depuis le début en changeant l'offset
- **Scalabilité** : ajout de consumers sans modifier le producer

La clé de partitionnement `station_code` garantit que les messages d'une même station arrivent dans l'ordre dans la même partition.

### Fréquence de collecte : 60 secondes

L'API Vélib' met à jour ses données environ toutes les 60 secondes. Collecter plus souvent produirait des doublons ; moins souvent ferait perdre des transitions d'état. 60s est le compromis optimal.

### PostgreSQL comme DWH

Pour ce volume (~2 M lignes/jour), PostgreSQL avec des **vues matérialisées** offre d'excellentes performances analytiques sans la complexité opérationnelle de ClickHouse. Le schéma peut évoluer vers ClickHouse sans modifier le producer ni le consumer.

### Modèle append-only pour raw

La table `raw.station_snapshots` est **append-only** : on n'écrase jamais les données brutes. Cela permet :
- De rejouer les transformations (idempotence)
- D'auditer l'historique complet
- De détecter des anomalies a posteriori
