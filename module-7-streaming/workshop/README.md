# Module 7 Homework Environment (PyFlink + Redpanda)

Ten katalog jest gotowy do wykonania Homework dla modułu 7.

## 1) Start środowiska

```bash
cd module-7-streaming/workshop
docker compose down -v
docker compose build
docker compose up -d
```

Sprawdzenie:

```bash
docker compose ps
```

Flink UI: http://localhost:8081

## 2) Pytanie 1: wersja Redpanda

```bash
docker exec -it workshop-redpanda-1 rpk version
```

## 3) Pytanie 2: producer do green-trips

Utwórz topic:

```bash
docker exec -it workshop-redpanda-1 rpk topic create green-trips
```

Wyślij dane:

```bash
uv sync
uv run python src/producers/producer_green.py
```

Skrypt wypisze czas wysyłki.

## 4) Pytanie 3: consumer trip_distance > 5

```bash
uv run python src/consumers/consumer_trip_distance.py
```

Skrypt wypisze liczbę rekordów z trip_distance > 5.0.

## 5) Tabele Postgres do Q4-Q6

```bash
docker exec -i workshop-postgres-1 psql -U postgres -d postgres < src/consumers/postgres_setup.sql
```

## 6) Uruchamianie jobów Flink

Przed każdym kolejnym pytaniem dobrze wyczyścić tabelę docelową i anulować poprzedni job w UI Flinka.

### Q4: tumbling 5 min per PULocationID

Uruchom job:

```bash
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/q4_tumbling_pu.py --pyFiles /opt/src -d
```

Po 1-2 minutach sprawdź wynik:

```sql
SELECT PULocationID, num_trips
FROM q4_tumbling_pu
ORDER BY num_trips DESC
LIMIT 3;
```

### Q5: session 5 min per PULocationID

Uruchom job:

```bash
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/q5_session_pu.py --pyFiles /opt/src -d
```

Sprawdź najdłuższą sesję:

```sql
SELECT PULocationID, num_trips
FROM q5_session_pu
ORDER BY num_trips DESC
LIMIT 1;
```

### Q6: tumbling 1 hour max tip_amount

Uruchom job:

```bash
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/q6_tumbling_tip.py --pyFiles /opt/src -d
```

Sprawdź godzinę z najwyższym napiwkiem:

```sql
SELECT window_start, total_tip
FROM q6_hourly_tip
ORDER BY total_tip DESC
LIMIT 1;
```

## 7) Przydatne operacje

Restart topicu bez duplikatów:

```bash
docker exec -it workshop-redpanda-1 rpk topic delete green-trips
docker exec -it workshop-redpanda-1 rpk topic create green-trips
```

Podgląd wiadomości:

```bash
docker exec -it workshop-redpanda-1 rpk topic consume green-trips -n 5
```
