import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

@click.command()
@click.option('--user', default='root')
@click.option('--password', default='root')
@click.option('--host', default='localhost')
@click.option('--port', default=5432, type=int)
@click.option('--db', default='ny_taxi')
@click.option('--table', default='yellow_taxi_trips')
@click.option(
    '--url',
    default='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'
)
@click.option('--chunksize', default=100_000)
def ingest_data(user, password, host, port, db, table, url, chunksize):

    print("Connecting to Postgres...")
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    print("Reading CSV in chunks...")
    df_iter = pd.read_csv(
        url,
        iterator=True,
        chunksize=chunksize,
        dtype=dtype,
        parse_dates=parse_dates
    )

    first = True

    for df_chunk in tqdm(df_iter, desc="Ingesting"):
        if first:
            df_chunk.head(0).to_sql(name=table, con=engine, if_exists='replace')
            first = False
            print(f"Created table {table}")

        df_chunk.to_sql(name=table, con=engine, if_exists='append')
        print(f"Inserted {len(df_chunk)} rows")

    print("Ingestion finished!")

if __name__ == "__main__":
    ingest_data()
