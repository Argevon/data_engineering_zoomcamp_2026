import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from google.cloud import storage


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required env var: {name}")
    return value


def fetch_json(url: str, client_identifier: str) -> dict:
    headers = {"Client-Identifier": client_identifier}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def upload_text_to_gcs(bucket_name: str, blob_name: str, content: str) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(content, content_type="application/json")


def upload_file_to_gcs(bucket_name: str, blob_name: str, local_file: Path) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(str(local_file))


def main() -> None:
    load_dotenv()

    _ = require_env("GCP_PROJECT_ID")
    bucket_name = require_env("GCS_BUCKET")
    base_url = require_env("MEVO_BASE_URL").rstrip("/")
    client_identifier = require_env("MEVO_CLIENT_IDENTIFIER")

    snapshot_ts = datetime.now(timezone.utc)
    dt = snapshot_ts.strftime("%Y-%m-%d")
    hh = snapshot_ts.strftime("%H")
    mm = snapshot_ts.strftime("%M")
    ts_compact = snapshot_ts.strftime("%Y%m%dT%H%M%SZ")

    status_url = f"{base_url}/station_status.json"
    info_url = f"{base_url}/station_information.json"

    status_payload = fetch_json(status_url, client_identifier)
    info_payload = fetch_json(info_url, client_identifier)

    status_blob = f"raw/station_status/dt={dt}/hh={hh}/mm={mm}/{ts_compact}.json"
    info_blob = f"raw/station_information/dt={dt}/hh={hh}/mm={mm}/{ts_compact}.json"

    upload_text_to_gcs(bucket_name, status_blob, json.dumps(status_payload, ensure_ascii=True))
    upload_text_to_gcs(bucket_name, info_blob, json.dumps(info_payload, ensure_ascii=True))

    stations = status_payload.get("data", {}).get("stations", [])
    df = pd.DataFrame(stations)
    df["snapshot_ts"] = snapshot_ts
    df["snapshot_date"] = snapshot_ts.date().isoformat()

    local_dir = Path("tmp")
    local_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = local_dir / f"station_status_{ts_compact}.parquet"
    df.to_parquet(parquet_path, index=False)

    bronze_blob = f"bronze/station_status/dt={dt}/hh={hh}/mm={mm}/{parquet_path.name}"
    upload_file_to_gcs(bucket_name, bronze_blob, parquet_path)

    print(
        json.dumps(
            {
                "bucket": bucket_name,
                "rows": len(df),
                "raw_status_blob": status_blob,
                "raw_info_blob": info_blob,
                "bronze_blob": bronze_blob,
            },
            ensure_ascii=True,
        )
    )


if __name__ == "__main__":
    main()
