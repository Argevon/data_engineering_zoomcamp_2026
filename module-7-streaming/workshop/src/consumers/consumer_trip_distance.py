from kafka import KafkaConsumer
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from models import green_ride_deserializer

TOPIC = "green-trips"
BROKER = "localhost:9092"


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BROKER],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="green-trips-distance-homework",
        value_deserializer=green_ride_deserializer,
        consumer_timeout_ms=10000,
    )

    count_gt_5 = 0
    total = 0

    for message in consumer:
        total += 1
        if message.value.trip_distance > 5.0:
            count_gt_5 += 1

    consumer.close()

    print(f"read {total} trips")
    print(f"trip_distance > 5.0: {count_gt_5}")


if __name__ == "__main__":
    main()
