-- =============================================================================
-- Initialisation du Data Warehouse Vélib' – PostgreSQL
-- =============================================================================
-- Modèle de données en deux couches (médaillon simplifié) :
--   • raw   – données brutes, append-only (source de vérité)
--   • mart  – données transformées et agrégées pour l'analyse
--
-- Choix de modélisation :
--   - PostgreSQL (vs ClickHouse) : volumes modérés (~1 500 stations × 1 msg/min
--     = ~2 M lignes/jour), SQL analytique classique, facilité de démarrage
--   - Séparation raw/mart : permet de rejouer les transformations sans
--     re-collecter les données sources
--   - Pas de dimension lente (SCD) nécessaire : les métadonnées des stations
--     sont stables et portées par chaque snapshot
-- =============================================================================

-- Extensions utiles
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- ── Schémas ───────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS mart;


-- =============================================================================
-- COUCHE RAW – Données brutes horodatées
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.station_snapshots (
    -- Clé de déduplication (station + horodatage de collecte)
    snapshot_id      BIGSERIAL PRIMARY KEY,

    -- Identité de la station
    station_code     VARCHAR(10)    NOT NULL,
    station_name     VARCHAR(200),

    -- Disponibilité instantanée
    bikes_available  SMALLINT       NOT NULL DEFAULT 0 CHECK (bikes_available >= 0),
    mechanical_bikes SMALLINT       NOT NULL DEFAULT 0 CHECK (mechanical_bikes >= 0),
    electric_bikes   SMALLINT       NOT NULL DEFAULT 0 CHECK (electric_bikes >= 0),
    docks_available  SMALLINT       NOT NULL DEFAULT 0 CHECK (docks_available >= 0),
    capacity         SMALLINT       NOT NULL DEFAULT 0 CHECK (capacity >= 0),

    -- État de la station
    is_installed     BOOLEAN        NOT NULL DEFAULT TRUE,
    is_renting       BOOLEAN        NOT NULL DEFAULT TRUE,
    is_returning     BOOLEAN        NOT NULL DEFAULT TRUE,

    -- Métriques calculées (enrichissement consumer)
    occupancy_rate   NUMERIC(5, 4),            -- 0.0000 → 1.0000
    station_status   VARCHAR(20),              -- 'en_service' | 'fermée' | 'hors_service'

    -- Géolocalisation
    latitude         DOUBLE PRECISION,
    longitude        DOUBLE PRECISION,

    -- Horodatages
    last_reported    TIMESTAMPTZ,              -- horodatage source (API Vélib')
    collected_at     TIMESTAMPTZ    NOT NULL,  -- horodatage producer (collecte réelle)
    inserted_at      TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

-- ── Index raw ─────────────────────────────────────────────────────────────────
-- Requêtes analytiques typiques : filtrage par station et par période
CREATE INDEX IF NOT EXISTS idx_raw_station_code
    ON raw.station_snapshots (station_code);

CREATE INDEX IF NOT EXISTS idx_raw_collected_at
    ON raw.station_snapshots (collected_at DESC);

CREATE INDEX IF NOT EXISTS idx_raw_station_time
    ON raw.station_snapshots (station_code, collected_at DESC);

-- Évite les doublons (même station, même horodatage de collecte)
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_station_snapshot
    ON raw.station_snapshots (station_code, collected_at);

COMMENT ON TABLE raw.station_snapshots IS
    'Données brutes des stations Vélib''. Un enregistrement = une mesure '
    'instantanée pour une station à un instant donné. Table append-only.';


-- =============================================================================
-- COUCHE MART – Données transformées pour l'analyse
-- =============================================================================

-- ── 1. Dernière valeur connue par station (vue opérationnelle) ────────────────

CREATE TABLE IF NOT EXISTS mart.station_latest (
    station_code     VARCHAR(10)    PRIMARY KEY,
    station_name     VARCHAR(200),
    bikes_available  SMALLINT,
    mechanical_bikes SMALLINT,
    electric_bikes   SMALLINT,
    docks_available  SMALLINT,
    capacity         SMALLINT,
    is_installed     BOOLEAN,
    is_renting       BOOLEAN,
    is_returning     BOOLEAN,
    occupancy_rate   NUMERIC(5, 4),
    station_status   VARCHAR(20),
    latitude         DOUBLE PRECISION,
    longitude        DOUBLE PRECISION,
    last_reported    TIMESTAMPTZ,
    collected_at     TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ    DEFAULT NOW()
);

COMMENT ON TABLE mart.station_latest IS
    'Dernière valeur connue pour chaque station. Mise à jour par upsert '
    'à chaque cycle de consommation Kafka.';


-- ── 2. Agrégats horaires ──────────────────────────────────────────────────────

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.hourly_station_stats AS
SELECT
    DATE_TRUNC('hour', collected_at)    AS hour_bucket,
    station_code,
    station_name,
    -- Disponibilité moyenne sur l'heure
    ROUND(AVG(bikes_available)::numeric, 2)   AS avg_bikes_available,
    ROUND(AVG(mechanical_bikes)::numeric, 2)  AS avg_mechanical_bikes,
    ROUND(AVG(electric_bikes)::numeric, 2)    AS avg_electric_bikes,
    ROUND(AVG(docks_available)::numeric, 2)   AS avg_docks_available,
    -- Taux d'occupation moyen
    ROUND(AVG(occupancy_rate)::numeric, 4)    AS avg_occupancy_rate,
    -- Extrêmes
    MIN(bikes_available)                      AS min_bikes_available,
    MAX(bikes_available)                      AS max_bikes_available,
    -- Nombre de snapshots (qualité du suivi)
    COUNT(*)                                  AS snapshot_count,
    -- Durée en saturation (docks_available = 0)
    COUNT(*) FILTER (WHERE docks_available = 0)  AS saturated_count,
    -- Durée à vide (bikes_available = 0)
    COUNT(*) FILTER (WHERE bikes_available = 0)  AS empty_count,
    -- Capacité (stable par station)
    MAX(capacity)                             AS capacity
FROM raw.station_snapshots
WHERE collected_at IS NOT NULL
GROUP BY DATE_TRUNC('hour', collected_at), station_code, station_name
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uq_mart_hourly
    ON mart.hourly_station_stats (hour_bucket, station_code);

COMMENT ON MATERIALIZED VIEW mart.hourly_station_stats IS
    'Agrégats horaires par station. Rafraîchi par Airflow toutes les 5 minutes.';


-- ── 3. Agrégats journaliers ───────────────────────────────────────────────────

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.daily_station_stats AS
SELECT
    DATE_TRUNC('day', collected_at)     AS day_bucket,
    station_code,
    station_name,
    ROUND(AVG(bikes_available)::numeric, 2)   AS avg_bikes_available,
    ROUND(AVG(mechanical_bikes)::numeric, 2)  AS avg_mechanical_bikes,
    ROUND(AVG(electric_bikes)::numeric, 2)    AS avg_electric_bikes,
    ROUND(AVG(occupancy_rate)::numeric, 4)    AS avg_occupancy_rate,
    MIN(bikes_available)                      AS min_bikes_available,
    MAX(bikes_available)                      AS max_bikes_available,
    COUNT(*)                                  AS snapshot_count,
    COUNT(*) FILTER (WHERE docks_available = 0)  AS saturated_count,
    COUNT(*) FILTER (WHERE bikes_available = 0)  AS empty_count,
    MAX(capacity)                             AS capacity
FROM raw.station_snapshots
GROUP BY DATE_TRUNC('day', collected_at), station_code, station_name
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS uq_mart_daily
    ON mart.daily_station_stats (day_bucket, station_code);


-- ── 4. Ranking des stations ───────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS mart.station_ranking (
    id               BIGSERIAL PRIMARY KEY,
    computed_at      TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    station_code     VARCHAR(10)    NOT NULL,
    station_name     VARCHAR(200),
    avg_occupancy_rate   NUMERIC(5, 4),
    avg_bikes_available  NUMERIC(8, 2),
    saturation_minutes   INTEGER,
    empty_minutes        INTEGER,
    rank_saturation      INTEGER,      -- 1 = station la plus saturée
    rank_availability    INTEGER,      -- 1 = station la plus souvent vide
    UNIQUE (computed_at, station_code)
);

COMMENT ON TABLE mart.station_ranking IS
    'Classement des stations calculé toutes les 5 minutes par Airflow. '
    'Permet d''identifier les stations chroniquement saturées ou vides.';


-- =============================================================================
-- Droits d'accès (lecture seule pour le rôle analytique)
-- =============================================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'analyst') THEN
        CREATE ROLE analyst LOGIN PASSWORD 'analyst';
    END IF;
END
$$;

GRANT USAGE ON SCHEMA raw, mart TO analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA raw TO analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA mart TO analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw  GRANT SELECT ON TABLES TO analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA mart GRANT SELECT ON TABLES TO analyst;
