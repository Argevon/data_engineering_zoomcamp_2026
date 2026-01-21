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
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime"
]

@click.command()
@click.option('--user', default='root')
@click.option('--password', default='root')
@click.option('--host', default='localhost')
@click.option('--port', default=5432, type=int)
@click.option('--db', default='ny_taxi')
@click.option('--table', default='green_tripdata_2025_11')
@click.option(
    '--url',
    default='https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet'
)
@click.option('--chunksize', default=100_000)
def ingest_green(user, password, host, port, db, table, url, chunksize):

    print("Connecting to Postgres...")
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    print("Reading Parquet in chunks...")
    df_iter = pd.read_parquet(url, engine='pyarrow')

    df_iter.to_sql(name=table, con=engine, if_exists='replace', index=False)
    print(f"Inserted {len(df_iter)} rows into {table}")

if __name__ == "__main__":
    ingest_green()
