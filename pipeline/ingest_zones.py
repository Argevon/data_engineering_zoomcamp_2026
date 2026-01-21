import pandas as pd
from sqlalchemy import create_engine
import click
import urllib.request

@click.command()
@click.option('--user', default='root')
@click.option('--password', default='root')
@click.option('--host', default='localhost')
@click.option('--port', default=5432, type=int)
@click.option('--db', default='ny_taxi')
@click.option('--table', default='taxi_zones')
@click.option('--url', default='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv')
def ingest_zones(user, password, host, port, db, table, url):

    print("Downloading Taxi Zones CSV...")
    local_file = "/tmp/taxi_zone_lookup.csv"
    urllib.request.urlretrieve(url, local_file)

    print("Connecting to Postgres...")
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    print("Reading CSV...")
    df = pd.read_csv(local_file)

    print(f"Creating table {table} in Postgres...")
    df.to_sql(name=table, con=engine, if_exists='replace', index=False)
    print(f"Inserted {len(df)} rows into {table}")

    print("Taxi Zones ingestion finished!")

if __name__ == "__main__":
    ingest_zones()
