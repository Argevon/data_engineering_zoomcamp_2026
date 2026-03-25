import os
from dotenv import load_dotenv
from google.cloud import bigquery


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

    # Load all parquet snapshots from bronze path.
    uri = f"gs://{bucket}/bronze/station_status/*/*/*/*.parquet"

    client = bigquery.Client(project=project_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()

    table = client.get_table(table_id)
    print(
        {
            "table_id": table_id,
            "rows_in_table": table.num_rows,
            "source_uri": uri,
            "job_id": load_job.job_id,
        }
    )


if __name__ == "__main__":
    main()
