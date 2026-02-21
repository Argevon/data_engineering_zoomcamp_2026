/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

# TODO: Set the asset name (recommended: staging.trips).
name: staging.trips
# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: bq.sql

# TODO: Declare dependencies so `bruin run ... --downstream` and lineage work.
# Examples:
# depends:
#   - ingestion.trips
#   - ingestion.payment_lookup
depends:
  - ingestion.trips
  - ingestion.payment_lookup

# TODO: Choose time-based incremental processing if the dataset is naturally time-windowed.
# - This module expects you to use `time_interval` to reprocess only the requested window.
materialization:
  # What is materialization?
  # Materialization tells Bruin how to turn your SELECT query into a persisted dataset.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  #
  # Materialization "type":
  # - table: persisted table
  # - view: persisted view (if the platform supports it)
  type: table
  # TODO: set a materialization strategy.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  # suggested strategy: time_interval
  #
  # Incremental strategies (what does "incremental" mean?):
  # Incremental means you update only part of the destination instead of rebuilding everything every run.
  # In Bruin, this is controlled by `strategy` plus keys like `incremental_key` and `time_granularity`.
  #
  # Common strategies you can choose from (see docs for full list):
  # - create+replace (full rebuild)
  # - truncate+insert (full refresh without drop/create)
  # - append (insert new rows only)
  # - delete+insert (refresh partitions based on incremental_key values)
  # - merge (upsert based on primary key)
  # - time_interval (refresh rows within a time window)
  strategy: time_interval
  # TODO: set incremental_key to your event time column (DATE or TIMESTAMP).
  incremental_key: pickup_datetime
  # TODO: choose `date` vs `timestamp` based on the incremental_key type.
  time_granularity: timestamp

# TODO: Define output columns, mark primary keys, and add a few checks.
columns:
  - name: pickup_datetime
    type: timestamp
    description: "Trip start time"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "Trip end time"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: trip_distance
    type: float64
    description: "Trip distance in miles"
    checks:
      - name: non_negative
  - name: fare_amount
    type: float64
    description: "Base fare in USD"
    checks:
      - name: non_negative
  - name: total_amount
    type: float64
    description: "Total fare amount in USD"
    checks:
      - name: non_negative
  - name: payment_type_name
    type: string
    description: "Payment method (from lookup table)"
    checks:
      - name: not_null
  - name: pickup_location_id
    type: int64
    description: "Pickup zone ID"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_location_id
    type: int64
    description: "Dropoff zone ID"
    primary_key: true
    nullable: false
    checks:
      - name: not_null

# TODO: Add one custom check that validates a staging invariant (uniqueness, ranges, etc.)
# Docs: https://getbruin.com/docs/bruin/quality/custom
custom_checks:
  - name: dropoff_after_pickup
    description: "Verify that dropoff time is after pickup time"
    query: |
      SELECT COUNT(*) as invalid_rows
      FROM staging.trips
      WHERE dropoff_datetime <= pickup_datetime
    value: 0

@bruin */

-- TODO: Write the staging SELECT query.
--
-- Purpose of staging:
-- - Clean and normalize schema from ingestion
-- - Deduplicate records (important if ingestion uses append strategy)
-- - Enrich with lookup tables (JOINs)
-- - Filter invalid rows (null PKs, negative values, etc.)
--
-- Why filter by {{ start_datetime }} / {{ end_datetime }}?
-- When using `time_interval` strategy, Bruin:
--   1. DELETES rows where `incremental_key` falls within the run's time window
--   2. INSERTS the result of your query
-- Therefore, your query MUST filter to the same time window so only that subset is inserted.
-- If you don't filter, you'll insert ALL data but only delete the window's data = duplicates.

WITH source_data AS (
  SELECT *
  FROM ingestion.trips
  WHERE CAST(tpep_pickup_datetime AS TIMESTAMP) >= CAST('{{ start_datetime }}' AS TIMESTAMP)
    AND CAST(tpep_pickup_datetime AS TIMESTAMP) < CAST('{{ end_datetime }}' AS TIMESTAMP)
),
deduplicated_trips AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY tpep_pickup_datetime, tpep_dropoff_datetime, fare_amount
      ORDER BY _dlt_load_id DESC
    ) AS rn
  FROM source_data
  WHERE tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
)
SELECT
  CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
  CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
  passenger_count,
  trip_distance,
  RatecodeID,
  store_and_fwd_flag,
  pulocationid AS pickup_location_id,
  dolocationid AS dropoff_location_id,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  total_amount,
  COALESCE(p.payment_type_name, 'Unknown') AS payment_type_name
FROM deduplicated_trips d
LEFT JOIN ingestion.payment_lookup p ON d.payment_type = p.payment_type_id
WHERE d.rn = 1
