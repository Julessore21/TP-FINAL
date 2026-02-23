-- =============================================================================
-- Indicateurs analytiques – Pipeline Vélib' Métropole
-- =============================================================================
-- Ces requêtes peuvent être exécutées directement sur le DWH PostgreSQL
-- (connexion : psql -h localhost -p 5433 -U velib -d velib_dwh)
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- INDICATEUR 1 : Vue d'ensemble en temps réel
-- Disponibilité globale du réseau à l'instant T
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    COUNT(*)                                          AS total_stations,
    COUNT(*) FILTER (WHERE station_status = 'en_service')  AS stations_actives,
    COUNT(*) FILTER (WHERE station_status = 'hors_service') AS stations_hors_service,
    COUNT(*) FILTER (WHERE bikes_available = 0)       AS stations_vides,
    COUNT(*) FILTER (WHERE docks_available = 0)       AS stations_saturees,
    SUM(bikes_available)                              AS total_velos_disponibles,
    SUM(mechanical_bikes)                             AS total_velosMeca,
    SUM(electric_bikes)                               AS total_velosElec,
    SUM(docks_available)                              AS total_bornes_libres,
    ROUND(AVG(occupancy_rate) * 100, 1)               AS taux_occupation_moyen_pct
FROM mart.station_latest
WHERE collected_at >= NOW() - INTERVAL '5 minutes';


-- ─────────────────────────────────────────────────────────────────────────────
-- INDICATEUR 2 : Top 10 des stations les plus saturées (24h glissantes)
-- Une station est saturée quand toutes ses bornes sont occupées (docks = 0)
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    station_code,
    station_name,
    avg_occupancy_rate * 100                          AS taux_occupation_moyen_pct,
    saturation_minutes                                AS minutes_en_saturation,
    -- Proportion du temps passé en saturation
    ROUND(saturation_minutes::numeric / NULLIF(
        EXTRACT(EPOCH FROM (NOW() - (NOW() - INTERVAL '24 hours'))) / 60,
    0) * 100, 1)                                      AS pct_temps_sature,
    rank_saturation
FROM mart.station_ranking
WHERE computed_at = (SELECT MAX(computed_at) FROM mart.station_ranking)
ORDER BY rank_saturation
LIMIT 10;


-- ─────────────────────────────────────────────────────────────────────────────
-- INDICATEUR 3 : Top 10 des stations les plus souvent vides (24h glissantes)
-- Utile pour identifier les stations mal réapprovisionnées
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    station_code,
    station_name,
    avg_bikes_available                               AS velos_dispo_moyen,
    empty_minutes                                     AS minutes_a_vide,
    rank_availability
FROM mart.station_ranking
WHERE computed_at = (SELECT MAX(computed_at) FROM mart.station_ranking)
ORDER BY rank_availability
LIMIT 10;


-- ─────────────────────────────────────────────────────────────────────────────
-- INDICATEUR 4 : Variation temporelle sur 24h (par heure)
-- Courbe d'utilisation du réseau heure par heure
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    hour_bucket,
    EXTRACT(HOUR FROM hour_bucket)                    AS heure,
    ROUND(AVG(avg_bikes_available), 1)                AS velos_dispo_moyen_reseau,
    ROUND(AVG(avg_occupancy_rate) * 100, 1)           AS taux_occupation_pct,
    SUM(saturated_count)                              AS nb_mesures_saturees,
    SUM(empty_count)                                  AS nb_mesures_vides,
    COUNT(DISTINCT station_code)                      AS nb_stations_actives
FROM mart.hourly_station_stats
WHERE hour_bucket >= NOW() - INTERVAL '24 hours'
GROUP BY hour_bucket
ORDER BY hour_bucket;


-- ─────────────────────────────────────────────────────────────────────────────
-- INDICATEUR 5 : Comparaison vélos mécaniques vs électriques
-- Répartition et taux d'utilisation par type de vélo
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    DATE_TRUNC('hour', collected_at)                  AS heure,
    SUM(mechanical_bikes)                             AS total_mecaniques_dispo,
    SUM(electric_bikes)                               AS total_electriques_dispo,
    SUM(mechanical_bikes + electric_bikes)            AS total_velos_dispo,
    -- Ratio électriques sur total
    ROUND(
        SUM(electric_bikes)::numeric /
        NULLIF(SUM(mechanical_bikes + electric_bikes), 0) * 100,
    1)                                                AS pct_velos_electriques,
    -- Stations avec uniquement des vélos électriques
    COUNT(*) FILTER (
        WHERE electric_bikes > 0 AND mechanical_bikes = 0
    )                                                 AS stations_elec_only
FROM raw.station_snapshots
WHERE collected_at >= NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', collected_at)
ORDER BY heure;


-- ─────────────────────────────────────────────────────────────────────────────
-- INDICATEUR 6 : Identification des stations "problématiques"
-- Stations souvent hors service ou avec données incohérentes
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    station_code,
    station_name,
    COUNT(*)                                          AS total_snapshots,
    COUNT(*) FILTER (WHERE NOT is_installed)          AS snapshots_hors_service,
    COUNT(*) FILTER (WHERE bikes_available > capacity) AS snapshots_incoherents,
    ROUND(
        COUNT(*) FILTER (WHERE NOT is_installed)::numeric
        / COUNT(*) * 100,
    1)                                                AS pct_hors_service,
    MIN(collected_at)                                 AS premier_snapshot,
    MAX(collected_at)                                 AS dernier_snapshot
FROM raw.station_snapshots
WHERE collected_at >= NOW() - INTERVAL '7 days'
GROUP BY station_code, station_name
HAVING COUNT(*) FILTER (WHERE NOT is_installed) > 0
ORDER BY pct_hors_service DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────
-- INDICATEUR 7 : Tendance journalière sur 7 jours
-- Vue macro de l'évolution du réseau
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    day_bucket                                        AS jour,
    ROUND(AVG(avg_bikes_available), 1)                AS velos_dispo_moyen,
    ROUND(AVG(avg_occupancy_rate) * 100, 1)           AS taux_occupation_pct,
    SUM(saturated_count)                              AS total_mesures_saturees,
    SUM(empty_count)                                  AS total_mesures_vides,
    COUNT(DISTINCT station_code)                      AS nb_stations_suivies
FROM mart.daily_station_stats
WHERE day_bucket >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY day_bucket
ORDER BY day_bucket;


-- ─────────────────────────────────────────────────────────────────────────────
-- INDICATEUR 8 : Heatmap heures × jours de la semaine
-- Patterns d'utilisation récurrents (ex: rush du matin/soir)
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    TO_CHAR(hour_bucket, 'Day')                       AS jour_semaine,
    EXTRACT(DOW FROM hour_bucket)                     AS num_jour,
    EXTRACT(HOUR FROM hour_bucket)                    AS heure,
    ROUND(AVG(avg_occupancy_rate) * 100, 1)           AS taux_occupation_pct,
    ROUND(AVG(avg_bikes_available), 1)                AS velos_dispo_moyen
FROM mart.hourly_station_stats
WHERE hour_bucket >= NOW() - INTERVAL '30 days'
GROUP BY
    TO_CHAR(hour_bucket, 'Day'),
    EXTRACT(DOW FROM hour_bucket),
    EXTRACT(HOUR FROM hour_bucket)
ORDER BY num_jour, heure;
