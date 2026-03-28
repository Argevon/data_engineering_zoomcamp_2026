SELECT
    snapshot_date,
    EXTRACT(HOUR FROM snapshot_ts)          AS hour,
    station_id,
    station_name,
    lat,
    lon,
    COUNT(*)                                AS snapshot_count,
    AVG(num_bikes_available)               AS avg_bikes_available,
    AVG(fill_rate)                          AS avg_fill_rate,
    MIN(num_bikes_available)               AS min_bikes,
    MAX(num_bikes_available)               AS max_bikes
FROM {{ ref('fct_station_snapshots') }}
GROUP BY 1, 2, 3, 4, 5, 6
