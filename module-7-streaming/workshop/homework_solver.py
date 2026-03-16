import pandas as pd

URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
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


def load_data() -> pd.DataFrame:
    df = pd.read_parquet(URL, columns=COLUMNS)
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    return df


def solve_q3(df: pd.DataFrame) -> int:
    return int((df["trip_distance"] > 5.0).sum())


def solve_q4(df: pd.DataFrame) -> tuple[int, int, str]:
    tmp = df[["lpep_pickup_datetime", "PULocationID"]].copy()
    tmp["window_start"] = tmp["lpep_pickup_datetime"].dt.floor("5min")
    grouped = (
        tmp.groupby(["window_start", "PULocationID"], as_index=False)
        .size()
        .rename(columns={"size": "num_trips"})
    )
    top = grouped.sort_values(["num_trips", "PULocationID"], ascending=[False, True]).iloc[0]
    return int(top["PULocationID"]), int(top["num_trips"]), str(top["window_start"])


def solve_q5(df: pd.DataFrame) -> tuple[int, int]:
    tmp = df[["PULocationID", "lpep_pickup_datetime"]].copy()
    tmp = tmp.sort_values(["PULocationID", "lpep_pickup_datetime"])

    # Start a new session when gap > 5 minutes for a given pickup location.
    gap = tmp.groupby("PULocationID")["lpep_pickup_datetime"].diff()
    new_session = (gap.isna()) | (gap > pd.Timedelta(minutes=5))
    tmp["session_id"] = new_session.groupby(tmp["PULocationID"]).cumsum()

    sessions = (
        tmp.groupby(["PULocationID", "session_id"], as_index=False)
        .size()
        .rename(columns={"size": "num_trips"})
    )
    top = sessions.sort_values(["num_trips", "PULocationID"], ascending=[False, True]).iloc[0]
    return int(top["PULocationID"]), int(top["num_trips"])


def solve_q6(df: pd.DataFrame) -> tuple[str, float]:
    tmp = df[["lpep_pickup_datetime", "tip_amount"]].copy()
    tmp["window_start"] = tmp["lpep_pickup_datetime"].dt.floor("1h")
    hourly = (
        tmp.groupby("window_start", as_index=False)["tip_amount"]
        .sum()
        .rename(columns={"tip_amount": "total_tip"})
    )
    top = hourly.sort_values(["total_tip", "window_start"], ascending=[False, True]).iloc[0]
    return str(top["window_start"]), float(top["total_tip"])


def main() -> None:
    df = load_data()

    q3 = solve_q3(df)
    q4_pu, q4_trips, q4_window = solve_q4(df)
    q5_pu, q5_trips = solve_q5(df)
    q6_hour, q6_tip = solve_q6(df)

    print("Q3 trip_distance > 5:", q3)
    print("Q4 top PULocationID:", q4_pu, "num_trips:", q4_trips, "window_start:", q4_window)
    print("Q5 longest session PULocationID:", q5_pu, "num_trips:", q5_trips)
    print("Q6 hour with largest total tip:", q6_hour, "total_tip:", f"{q6_tip:.2f}")


if __name__ == "__main__":
    main()
