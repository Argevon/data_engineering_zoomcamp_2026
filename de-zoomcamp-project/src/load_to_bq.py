import os
import re
from datetime import datetime, timezone

from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud import storage


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required env var: {name}")
    return value


def extract_snapshot_ts(blob_name: str) -> datetime | None:
    match = re.search(r"station_status_(\d{8}T\d{6}Z)\.parquet$", blob_name)
    if not match:
        return None
    return datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)


def get_table_max_snapshot_ts(bq_client: bigquery.Client, table_id: str) -> datetime | None:
    query = f"SELECT MAX(snapshot_ts) AS max_snapshot_ts FROM `{table_id}`"
    try:
        result = bq_client.query(query).result()
    except NotFound:
        return None

    row = next(iter(result), None)
    if not row:
        return None
    return row.max_snapshot_ts


def main() -> None:
    load_dotenv()

    project_id = require_env("GCP_PROJECT_ID")
    bucket = require_env("GCS_BUCKET")
    dataset = require_env("BQ_DATASET")

    table_id = f"{project_id}.{dataset}.station_status_snapshots"

    bq_client = bigquery.Client(project=project_id)
    storage_client = storage.Client(project=project_id)

    prefix = "bronze/station_status/"
    blobs = [blob for blob in storage_client.list_blobs(bucket, prefix=prefix) if blob.name.endswith(".parquet")]

    if not blobs:
        print({"message": "No parquet files found - skipping load.", "table_id": table_id})
        return

    latest_blob = max(blobs, key=lambda blob: blob.name)
    latest_blob_ts = extract_snapshot_ts(latest_blob.name)
    max_loaded_ts = get_table_max_snapshot_ts(bq_client, table_id)

    if latest_blob_ts and max_loaded_ts and latest_blob_ts <= max_loaded_ts:
        print(
            {
                "message": "Latest snapshot already loaded - skipping append.",
                "table_id": table_id,
                "latest_blob": latest_blob.name,
                "latest_blob_ts": latest_blob_ts.isoformat(),
                "max_loaded_ts": max_loaded_ts.isoformat(),
            }
        )
    else:
        status_uri = f"gs://{bucket}/{latest_blob.name}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True,
        )

        load_job = bq_client.load_table_from_uri(status_uri, table_id, job_config=job_config)
        load_job.result()

        table = bq_client.get_table(table_id)
        print(
            {
                "table_id": table_id,
                "rows_in_table": table.num_rows,
                "source_file": latest_blob.name,
                "job_id": load_job.job_id,
            }
        )

    info_prefix = "bronze/station_information/"
    info_blobs = storage_client.list_blobs(bucket, prefix=info_prefix)
    info_uris = [f"gs://{bucket}/{blob.name}" for blob in info_blobs if blob.name.endswith(".parquet")]

    if not info_uris:
        return

    info_table_id = f"{project_id}.{dataset}.station_information"
    info_job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    info_job = bq_client.load_table_from_uri(info_uris, info_table_id, job_config=info_job_config)
    info_job.result()

    info_table = bq_client.get_table(info_table_id)
    print(
        {
            "table_id": info_table_id,
            "rows_in_table": info_table.num_rows,
            "source_files": len(info_uris),
            "job_id": info_job.job_id,
        }
    )


if __name__ == "__main__":
    main()
