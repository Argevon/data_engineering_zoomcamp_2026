from time import time
from pathlib import Path
import sys

import pandas as pd
from kafka import KafkaProducer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from models import green_ride_from_row, green_ride_serializer

TOPIC = "green-trips"
BROKER = "localhost:9092"
DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
COLUMNS = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "tip_amount",
    "total_amount",
]


def main():
    df = pd.read_parquet(DATA_URL, columns=COLUMNS)

    producer = KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=green_ride_serializer,
    )

    t0 = time()
    for _, row in df.iterrows():
        ride = green_ride_from_row(row)
        producer.send(TOPIC, value=ride)

    producer.flush()
    t1 = time()

    print(f"sent {len(df)} rows to {TOPIC}")
    print(f"took {(t1 - t0):.2f} seconds")


if __name__ == "__main__":
    main()
