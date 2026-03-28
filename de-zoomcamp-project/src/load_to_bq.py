import os

from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud import storage


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required env var: {name}")
    return value


def main() -> None:
    load_dotenv()

    project_id = require_env("GCP_PROJECT_ID")
    bucket = require_env("GCS_BUCKET")
    dataset = require_env("BQ_DATASET")

    table_id = f"{project_id}.{dataset}.station_status_snapshots"

    bq_client = bigquery.Client(project=project_id)
    storage_client = storage.Client(project=project_id)

    prefix = "bronze/station_status/"
    blobs = storage_client.list_blobs(bucket, prefix=prefix)
    uris = [f"gs://{bucket}/{blob.name}" for blob in blobs if blob.name.endswith(".parquet")]

    if not uris:
        print({"message": "No parquet files found - skipping load.", "table_id": table_id})
        return

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    load_job = bq_client.load_table_from_uri(uris, table_id, job_config=job_config)
    load_job.result()

    table = bq_client.get_table(table_id)
    print(
        {
            "table_id": table_id,
            "rows_in_table": table.num_rows,
            "source_files": len(uris),
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
