#!/usr/bin/env python3
"""Upload downloaded NYC TLC files to GCS and load into BigQuery.

Workflow per file:
  - upload local .csv.gz to gs://{bucket}/{taxi}/{year}/{filename}
  - load into staging table `{dataset}.{taxi}_tripdata_{year}_{month}` (autodetect CSV)
  - create/merge into `{dataset}.{taxi}_tripdata` using MD5 unique id

Requires `google-cloud-storage` and `google-cloud-bigquery`.
Authentication: set `GOOGLE_APPLICATION_CREDENTIALS` or use gcloud login.
"""
import argparse
import csv
import gzip
import re
from pathlib import Path
from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound
import sys


FILENAME_RE = re.compile(r"^(yellow|green|fhv)_tripdata_(\d{4})-(\d{2})\.csv(\.gz)?$")

TYPE_MAP = {
    # Common identifiers
    "vendorid": "INTEGER",
    "ratecodeid": "INTEGER",
    "pulocationid": "INTEGER",
    "dolocationid": "INTEGER",
    "passenger_count": "INTEGER",
    "trip_type": "INTEGER",
    "payment_type": "INTEGER",
    # Timestamps
    "tpep_pickup_datetime": "TIMESTAMP",
    "tpep_dropoff_datetime": "TIMESTAMP",
    "lpep_pickup_datetime": "TIMESTAMP",
    "lpep_dropoff_datetime": "TIMESTAMP",
    # Strings
    "store_and_fwd_flag": "STRING",
    # Numerics / monetary values
    "trip_distance": "NUMERIC",
    "fare_amount": "NUMERIC",
    "extra": "NUMERIC",
    "mta_tax": "NUMERIC",
    "tip_amount": "NUMERIC",
    "tolls_amount": "NUMERIC",
    "ehail_fee": "NUMERIC",
    "improvement_surcharge": "NUMERIC",
    "total_amount": "NUMERIC",
    "congestion_surcharge": "NUMERIC",
    "airport_fee": "NUMERIC",
    # FHV columns
    "dispatching_base_num": "STRING",
    "pickup_datetime": "TIMESTAMP",
    "dropoff_datetime": "TIMESTAMP",
    "sr_flag": "INTEGER",
    "affiliated_base_number": "STRING",
}


def create_bucket_if_not_exists(storage_client: storage.Client, bucket_name: str, project: str) -> None:
    """Create GCS bucket if it doesn't exist."""
    bucket = storage_client.bucket(bucket_name)
    if bucket.exists():
        print(f"Bucket already exists: gs://{bucket_name}")
        return
    
    try:
        bucket = storage_client.create_bucket(bucket_name, project=project)
        print(f"Created bucket: gs://{bucket_name}")
    except Exception as e:
        print(f"Failed to create bucket: {e}")
        raise


def create_dataset_if_not_exists(bq_client: bigquery.Client, project: str, dataset: str) -> None:
    """Create BigQuery dataset if it doesn't exist."""
    dataset_id = f"{project}.{dataset}"
    try:
        bq_client.get_dataset(dataset_id)
        print(f"Dataset already exists: {dataset_id}")
        return
    except NotFound:
        pass
    
    try:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset = bq_client.create_dataset(dataset)
        print(f"Created dataset: {dataset_id}")
    except Exception as e:
        print(f"Failed to create dataset: {e}")
        raise


def upload_file(storage_client: storage.Client, bucket_name: str, src_path: Path, dst_path: str) -> str:
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(dst_path)
    if blob.exists():
        print(f"GCS exists, skipping upload: gs://{bucket_name}/{dst_path}")
        return f"gs://{bucket_name}/{dst_path}"

    print(f"Uploading {src_path} -> gs://{bucket_name}/{dst_path}")
    blob.upload_from_filename(str(src_path))
    return f"gs://{bucket_name}/{dst_path}"


def load_csv_to_bq(bq_client: bigquery.Client, gcs_uri: str, project: str, dataset: str, table_id: str):
    table_ref = f"{project}.{dataset}.{table_id}"
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    print(f"Starting load job {gcs_uri} -> {table_ref}")
    load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()
    print(f"Loaded to {table_ref} ({load_job.output_rows} rows, job {load_job.job_id})")


def read_csv_header(path: Path) -> list[str]:
    if path.suffix == ".gz":
        opener = gzip.open
    else:
        opener = open

    with opener(path, "rt", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        return next(reader)


def build_schema_from_header(header: list[str]) -> list[bigquery.SchemaField]:
    schema = []
    for col in header:
        col_key = col.strip().lower()
        field_type = TYPE_MAP.get(col_key, "STRING")
        schema.append(bigquery.SchemaField(col, field_type))
    return schema


def run_merge(bq_client: bigquery.Client, project: str, dataset: str, taxi: str, year: str, month: str, staging_table: str):
    main_table = f"{project}.{dataset}.{taxi}_tripdata"
    staging = f"{project}.{dataset}.{staging_table}"

    if taxi == 'yellow':
        pickup = 'tpep_pickup_datetime'
        dropoff = 'tpep_dropoff_datetime'
    else:
        pickup = 'lpep_pickup_datetime'
        dropoff = 'lpep_dropoff_datetime'

    staging_with_id = f"{staging}_with_id"

    create_id_sql = f"""
    CREATE OR REPLACE TABLE `{staging_with_id}` AS
    SELECT
      MD5(CONCAT(
        COALESCE(CAST(VendorID AS STRING), ''),
        COALESCE(CAST({pickup} AS STRING), ''),
        COALESCE(CAST({dropoff} AS STRING), ''),
        COALESCE(CAST(PULocationID AS STRING), ''),
        COALESCE(CAST(DOLocationID AS STRING), '')
      )) AS unique_row_id,
      '{taxi}_tripdata_{year}-{month}.csv' AS filename,
      *
    FROM `{staging}`;
    """

    merge_sql = f"""
    CREATE TABLE IF NOT EXISTS `{main_table}` AS
    SELECT * FROM `{staging_with_id}` WHERE FALSE;

    MERGE INTO `{main_table}` T
    USING `{staging_with_id}` S
    ON T.unique_row_id = S.unique_row_id
    WHEN NOT MATCHED THEN
      INSERT ROW
    ;
    """

    print("Creating staging with unique ids...")
    q = bq_client.query(create_id_sql)
    q.result()

    print("Merging into main table...")
    q2 = bq_client.query(merge_sql)
    q2.result()
    print("Merge complete.")


def main():
    parser = argparse.ArgumentParser(description="Upload local NYC taxi files to GCS and load into BigQuery")
    parser.add_argument('--local-dir', default='./nyc_taxi_data', help='Local directory with downloaded files')
    parser.add_argument('--bucket', required=True, help='GCS bucket name')
    parser.add_argument('--project', required=True, help='GCP project id')
    parser.add_argument('--dataset', required=True, help='BigQuery dataset')
    parser.add_argument('--skip-upload', action='store_true', help='Skip uploading and only run BQ load from existing GCS URIs')
    parser.add_argument('--gcs-prefix', default='', help='Optional prefix under bucket when skipping upload (e.g. taxi/)')
    parser.add_argument('--mode', choices=['as-is', 'merge'], default='as-is', help='Load mode: "as-is" loads each file into its own table; "merge" runs dedup/merge into consolidated table')
    args = parser.parse_args()

    storage_client = storage.Client()
    bq_client = bigquery.Client()
    # Create bucket and dataset if they don't exist
    print(f"\nChecking/creating GCS bucket and BigQuery dataset...")
    create_bucket_if_not_exists(storage_client, args.bucket, args.project)
    create_dataset_if_not_exists(bq_client, args.project, args.dataset)
    print()


    local = Path(args.local_dir)
    if not local.exists():
        print(f"Local dir not found: {local}")
        raise SystemExit(1)

    files = list(local.rglob('*.csv*'))
    if not files:
        print("No csv files found in local dir")
        raise SystemExit(1)

    for f in files:
        fname = f.name
        m = FILENAME_RE.match(fname)
        if not m:
            print(f"Skipping unrecognized filename: {fname}")
            continue
        taxi, year, month, _ = m.groups()
        dst_path = f"{taxi}/{year}/{fname}"
        gcs_uri = f"gs://{args.bucket}/{dst_path}"

        if not args.skip_upload:
            try:
                upload_file(storage_client, args.bucket, f, dst_path)
            except Exception as e:
                print(f"Upload failed for {f}: {e}")
                continue

        # Load to staging table
        staging_table = f"{taxi}_tripdata_{year}_{month}"
        try:
            schema_fields = None
            try:
                header = read_csv_header(f)
                schema_fields = build_schema_from_header(header)
            except Exception as e:
                print(f"Failed to build schema from {f}: {e}. Falling back to autodetect.")

            if schema_fields:
                job_config = bigquery.LoadJobConfig()
                job_config.source_format = bigquery.SourceFormat.CSV
                job_config.skip_leading_rows = 1
                job_config.autodetect = False
                job_config.schema = schema_fields
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
                table_ref = f"{args.project}.{args.dataset}.{staging_table}"
                print(f"Starting load job {gcs_uri} -> {table_ref} (schema from file)")
                load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
                load_job.result()
                print(f"Loaded to {table_ref} ({load_job.output_rows} rows, job {load_job.job_id})")
            else:
                load_csv_to_bq(bq_client, gcs_uri, args.project, args.dataset, staging_table)
            if args.mode == 'merge':
                run_merge(bq_client, args.project, args.dataset, taxi, year, month, staging_table)
            else:
                print(f"Loaded as-is into {args.project}.{args.dataset}.{staging_table}")
        except Exception as e:
            action = 'load/merge' if args.mode == 'merge' else 'load'
            print(f"BQ {action} failed for {gcs_uri}: {e}")


if __name__ == '__main__':
    main()
