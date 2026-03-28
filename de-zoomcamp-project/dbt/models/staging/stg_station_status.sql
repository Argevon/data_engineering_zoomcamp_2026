SELECT
    CAST(station_id AS STRING)              AS station_id,
    CAST(num_bikes_available AS INT64)      AS num_bikes_available,
    CAST(num_docks_available AS INT64)      AS num_docks_available,
    CAST(is_installed AS BOOL)              AS is_installed,
    CAST(is_renting AS BOOL)               AS is_renting,
    CAST(is_returning AS BOOL)             AS is_returning,
    CAST(snapshot_ts AS TIMESTAMP)         AS snapshot_ts,
    CAST(snapshot_date AS DATE)            AS snapshot_date
FROM {{ source('mevo', 'station_status_snapshots') }}
WHERE CAST(is_installed AS BOOL) = TRUE
