{{
    config(
        materialized='incremental',
        unique_key=['station_id', 'snapshot_ts'],
        incremental_strategy='merge'
    )
}}

SELECT
    s.station_id,
    s.num_bikes_available,
    s.num_docks_available,
    s.is_installed,
    s.is_renting,
    s.is_returning,
    s.snapshot_ts,
    s.snapshot_date,
    i.name                                                  AS station_name,
    i.lat,
    i.lon,
    i.capacity,
    SAFE_DIVIDE(s.num_bikes_available, i.capacity)         AS fill_rate
FROM {{ ref('stg_station_status') }} s
LEFT JOIN {{ ref('stg_station_information') }} i USING (station_id)

{% if is_incremental() %}
WHERE s.snapshot_ts > (SELECT COALESCE(MAX(snapshot_ts), TIMESTAMP('1970-01-01')) FROM {{ this }})
{% endif %}
