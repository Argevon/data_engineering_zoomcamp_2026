CREATE SCHEMA IF NOT EXISTS `okok-aeced.de_zoomcamp_project`;

CREATE TABLE IF NOT EXISTS `okok-aeced.de_zoomcamp_project.station_status_snapshots`
(
  snapshot_ts TIMESTAMP,
  snapshot_date DATE,
  station_id STRING,
  num_bikes_available INT64,
  num_docks_available INT64,
  is_installed INT64,
  is_renting INT64,
  is_returning INT64,
  last_reported INT64
)
PARTITION BY snapshot_date
CLUSTER BY station_id;

CREATE TABLE IF NOT EXISTS `okok-aeced.de_zoomcamp_project.station_information`
(
  station_id STRING,
  name STRING,
  address STRING,
  lat FLOAT64,
  lon FLOAT64,
  capacity INT64,
  rental_uris STRING,
  last_ingested_ts TIMESTAMP
);
