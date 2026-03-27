# MEVO Availability Analytics - VM Deployment

## Wymagania na VM
- Docker
- Docker Compose v2
- Git
- Port 8080 otwarty w firewall

## Deploy krok po kroku

### 1. Klonowanie repo
```bash
git clone https://github.com/Argevon/data_engineering_zoomcamp_2026.git
cd data_engineering_zoomcamp_2026/de-zoomcamp-project
```

### 2. Skopiuj klucz GCP na VM
```bash
# Z lokalnej maszyny:
scp /ścieżka/do/okok-aeced-8c5e16db967c.json user@<VM_IP>:~/data_engineering_zoomcamp_2026/module-4-analytics-engineering/
```

### 3. Utwórz .env na VM
```bash
cat > .env << 'EOF'
GCP_PROJECT_ID=okok-aeced
GCP_REGION=europe-west2
GCS_BUCKET=de_zoomcamp_project_bucket
BQ_DATASET=de_zoomcamp_project
MEVO_CLIENT_IDENTIFIER=argevon-mevo-analytics
MEVO_BASE_URL=https://gbfs.urbansharing.com/rowermevo.pl
GOOGLE_APPLICATION_CREDENTIALS=/workspaces/data_engineering_zoomcamp_2026/module-4-analytics-engineering/okok-aeced-8c5e16db967c.json
EOF
```

### 4. Inicjalizacja bazy Airflow
```bash
cd airflow
AIRFLOW_UID=$(id -u) docker compose up airflow-init
```

### 5. Start Airflow
```bash
AIRFLOW_UID=$(id -u) docker compose up -d
docker compose ps
```

### 6. Otwórz UI
- URL: http://<VM_IP>:8080
- login: admin
- hasło: admin

### 7. Włącz DAG
- Wejdź w DAG: `mevo_ingestion_to_bq`
- Włącz toggle (unpause)
- Kliknij `Trigger DAG` dla pierwszego uruchomienia
- Dalej odpala się automatycznie co 5 minut

### 8. Sprawdź dane w BigQuery
```bash
bq --project_id=okok-aeced query --use_legacy_sql=false \
'SELECT snapshot_date, COUNT(*) AS rows_cnt
 FROM `okok-aeced.de_zoomcamp_project.station_status_snapshots`
 GROUP BY 1 ORDER BY 1 DESC LIMIT 10;'
```

## Stop
```bash
cd airflow
AIRFLOW_UID=$(id -u) docker compose down
```

## Rebuild po zmianach w src/
```bash
cd airflow
AIRFLOW_UID=$(id -u) docker compose build
AIRFLOW_UID=$(id -u) docker compose up -d
```
