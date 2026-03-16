up:
	docker compose up -d --build

down:
	docker compose down

down-v:
	docker compose down -v

topic-create:
	docker exec -it workshop-redpanda-1 rpk topic create green-trips

topic-recreate:
	docker exec -it workshop-redpanda-1 rpk topic delete green-trips || true
	docker exec -it workshop-redpanda-1 rpk topic create green-trips

produce:
	uv sync
	uv run python src/producers/producer_green.py

consume:
	uv run python src/consumers/consumer_trip_distance.py

psql:
	docker exec -it workshop-postgres-1 psql -U postgres -d postgres
