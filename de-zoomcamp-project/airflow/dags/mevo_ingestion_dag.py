from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="mevo_ingestion_to_bq",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 3, 25),
    catchup=False,
    tags=["mevo"],
) as dag:
    ingest_mevo = BashOperator(
        task_id="ingest_mevo",
        bash_command="echo 'mevo ingest placeholder'",
    )

    load_to_bq = BashOperator(
        task_id="load_to_bq",
        bash_command="echo 'bq load placeholder'",
    )

    ingest_mevo >> load_to_bq
