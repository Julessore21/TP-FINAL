# Document d'architecture – Pipeline Vélib' Métropole

## 1. Objectif

Ce document décrit l'architecture complète de la pipeline de données temps réel
construite pour analyser la disponibilité des stations Vélib' Métropole.

---

## 2. Vue d'ensemble de l'architecture

L'architecture suit le modèle **Lambda / Streaming** adapté aux volumes modérés :

```
Source → Streaming (Kafka) → Orchestration (Airflow) → DWH (PostgreSQL) → Analyse (SQL)
```

Chaque composant est conteneurisé via Docker, ce qui garantit la reproductibilité
de l'environnement et simplifie le déploiement.

---

## 3. Composants et leur rôle

### 3.1 API Vélib' Métropole (source)

- **URL** : `https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records`
- **Format** : JSON paginé (100 résultats/page)
- **Mise à jour** : ~60 secondes
- **Données** : ~1 500 stations, état temps réel

### 3.2 Producer Python (ingestion)

Responsabilités :
1. Interroger l'API toutes les 60 secondes
2. Paginer pour récupérer l'intégralité des stations (~1 500)
3. Normaliser et publier chaque station comme message JSON dans Kafka
4. Gérer les erreurs réseau avec retry

**Gestion des erreurs :**
- Timeout réseau → log d'erreur, attente du prochain cycle (pas de crash)
- Kafka indisponible au démarrage → retry avec backoff exponentiel (10 tentatives)
- Erreur HTTP API → log d'erreur, cycle interrompu

### 3.3 Kafka (streaming)

**Rôle** : découpler le producer (ingestion) du consumer (traitement).

Configuration :
- **Topic** : `velib-stations`
- **Partitions** : 1 (suffisant pour ce volume, extensible)
- **Clé de partition** : `station_code` (ordonnancement local par station)
- **Rétention** : 24h / 1 GB (permet le replay sur 24h)
- **Compression** : gzip (réduction de la bande passante ~40%)

**Intérêt de Kafka ici :**
- Si le DWH tombe, les données s'accumulent dans le topic et sont consommées
  dès le retour à la normale (pas de perte)
- Le producer ignore totalement comment les données seront traitées
  (extensibilité : ajout d'un 2e consumer pour un autre usage sans toucher le producer)

### 3.4 Consumer Python (transformation)

Invoqué par Airflow (pas un daemon continu) pour garantir le contrôle de l'orchestration.

Transformations appliquées :
- **Validation** : rejet des messages sans `station_code`
- **Enrichissement** :
  - `occupancy_rate` = bikes_available / capacity
  - `station_status` = 'en_service' | 'fermée' | 'hors_service'
- **Chargement** :
  - `raw.station_snapshots` : INSERT (append-only)
  - `mart.station_latest` : UPSERT (dernière valeur)

### 3.5 Airflow (orchestration)

Orchestre l'ensemble du pipeline en mode **batch micro-streaming** (toutes les 5 min).

Avantages de ce choix vs consumer daemon continu :
- Visibilité complète des exécutions (succès, échecs, durées)
- Gestion des dépendances (ne pas agréger si la consommation a échoué)
- Retry automatique configurable par tâche
- Planification temporelle claire

### 3.6 PostgreSQL DWH (stockage analytique)

**Schéma médaillon simplifié :**

```
Bronze (raw)   → raw.station_snapshots   (données brutes, append-only)
Silver (mart)  → mart.station_latest     (dernière valeur, upsert)
Gold (mart)    → mart.hourly/daily_stats (agrégats, vues matérialisées)
               → mart.station_ranking    (indicateurs calculés)
```

---

## 4. Séquence d'exécution complète

```
T+0:00  Producer collecte l'API Vélib'
T+0:05  Producer publie ~1500 messages dans Kafka
T+0:00  Airflow démarre le cycle (toutes les 5 min)
T+0:01  check_kafka_health → OK
T+0:01  check_dwh_health → OK
T+0:02  consume_kafka_batch → lit ~5000 messages → insère raw + mart
T+0:03  refresh_hourly_agg → REFRESH MATERIALIZED VIEW
T+0:03  refresh_daily_agg → REFRESH MATERIALIZED VIEW
T+0:04  compute_station_rank → calcul ranking 24h
T+0:04  data_quality_check → contrôles cohérence
T+0:05  Cycle suivant démarre
```

---

## 5. Considérations de production

### Scalabilité
- **Producer** : passage multi-thread possible si l'API supporte des appels parallèles
- **Kafka** : ajout de partitions et de brokers sans interruption
- **Consumer** : ajout de consumers dans le même `group_id` pour paralléliser
- **DWH** : migration vers ClickHouse possible sans modifier producer/consumer

### Résilience
- Kafka : rétention 24h = buffer en cas de panne DWH
- Airflow : retry automatique + alertes email configurables
- Consumer : idempotent (unique constraint sur `station_code, collected_at`)

### Qualité des données
- Validation à l'entrée (consumer)
- Contrôles de cohérence (data_quality_check Airflow)
- Table raw immuable (audit trail complet)
