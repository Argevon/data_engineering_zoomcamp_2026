from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "de-zoomcamp",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="mevo_ingestion_to_bq",
    default_args=DEFAULT_ARGS,
    description="Fetch MEVO GBFS data every 5 min, store in GCS, load to BigQuery",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 3, 25),
    catchup=False,
    max_active_runs=1,
    tags=["mevo", "gcs", "bq"],
) as dag:
    ingest_mevo = BashOperator(
        task_id="ingest_mevo",
        bash_command="cd /opt/mevo_project && python src/ingest_mevo.py",
    )

    load_to_bq = BashOperator(
        task_id="load_to_bq",
        bash_command="cd /opt/mevo_project && python src/load_to_bq.py",
    )

    ingest_mevo >> load_to_bq
