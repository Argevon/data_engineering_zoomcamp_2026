/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# TODO: Set the asset name (recommended: reports.trips_report).
name: reports.trips_report

# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: bq.sql

# TODO: Declare dependency on the staging asset(s) this report reads from.
depends:
  - staging.trips

# TODO: Choose materialization strategy.
# For reports, `time_interval` is a good choice to rebuild only the relevant time window.
# Important: Use the same `incremental_key` as staging (e.g., pickup_datetime) for consistency.
materialization:
  type: table
  # suggested strategy: time_interval
  strategy: time_interval
  # TODO: set to your report's date column
  incremental_key: report_date
  # TODO: set to `date` or `timestamp`
  time_granularity: date

# TODO: Define report columns + primary key(s) at your chosen level of aggregation.
columns:
  - name: report_date
    type: date
    description: "Trip date (derived from pickup_datetime)"
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Payment method"
    primary_key: true
    checks:
      - name: not_null
  - name: trip_count
    type: int64
    description: "Total number of trips"
    checks:
      - name: non_negative
  - name: total_revenue
    type: float64
    description: "Total revenue in USD"
    checks:
      - name: non_negative
  - name: avg_fare
    type: float64
    description: "Average fare amount"
    checks:
      - name: non_negative
  - name: avg_trip_distance
    type: float64
    description: "Average trip distance"
    checks:
      - name: non_negative
  - name: avg_passenger_count
    type: float64
    description: "Average passengers per trip"
    checks:
      - name: non_negative

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT
  CAST(s.pickup_datetime AS DATE) AS report_date,
  s.payment_type_name,
  COUNT(*) AS trip_count,
  SUM(s.total_amount) AS total_revenue,
  AVG(s.fare_amount) AS avg_fare,
  AVG(s.trip_distance) AS avg_trip_distance,
  AVG(s.passenger_count) AS avg_passenger_count
FROM staging.trips s
WHERE CAST(s.pickup_datetime AS DATE) >= CAST('{{ start_datetime }}' AS DATE)
  AND CAST(s.pickup_datetime AS DATE) < CAST('{{ end_datetime }}' AS DATE)
GROUP BY
  CAST(s.pickup_datetime AS DATE),
  s.payment_type_name
