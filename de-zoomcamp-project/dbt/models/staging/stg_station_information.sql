SELECT DISTINCT
    CAST(station_id AS STRING)   AS station_id,
    name,
    CAST(lat AS FLOAT64)         AS lat,
    CAST(lon AS FLOAT64)         AS lon,
    CAST(capacity AS INT64)      AS capacity
FROM {{ source('mevo', 'station_information') }}
WHERE station_id IS NOT NULL
