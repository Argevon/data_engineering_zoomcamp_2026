import json
from dataclasses import dataclass


@dataclass
class GreenRide:
    lpep_pickup_datetime: str
    lpep_dropoff_datetime: str
    PULocationID: int
    DOLocationID: int
    passenger_count: float
    trip_distance: float
    tip_amount: float
    total_amount: float


def green_ride_from_row(row):
    return GreenRide(
        lpep_pickup_datetime=str(row["lpep_pickup_datetime"]),
        lpep_dropoff_datetime=str(row["lpep_dropoff_datetime"]),
        PULocationID=int(row["PULocationID"]),
        DOLocationID=int(row["DOLocationID"]),
        passenger_count=float(row["passenger_count"]),
        trip_distance=float(row["trip_distance"]),
        tip_amount=float(row["tip_amount"]),
        total_amount=float(row["total_amount"]),
    )


def green_ride_serializer(ride: GreenRide) -> bytes:
    return json.dumps(ride.__dict__).encode("utf-8")


def green_ride_deserializer(data: bytes) -> GreenRide:
    payload = json.loads(data.decode("utf-8"))
    return GreenRide(**payload)
