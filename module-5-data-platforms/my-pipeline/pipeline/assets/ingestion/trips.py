"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Set the connection.
connection: gcp-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: VendorID
    type: int64
    description: "Taxi vendor ID"
  - name: pickup_datetime
    type: timestamp
    description: "Trip start time"
  - name: dropoff_datetime
    type: timestamp
    description: "Trip end time"
  - name: passenger_count
    type: int64
    description: "Number of passengers"
  - name: trip_distance
    type: float64
    description: "Trip distance in miles"
  - name: RatecodeID
    type: int64
    description: "Rate code ID"
  - name: store_and_fwd_flag
    type: string
    description: "Store and forward flag (Y/N)"
  - name: PULocationID
    type: int64
    description: "Pickup location ID"
  - name: DOLocationID
    type: int64
    description: "Dropoff location ID"
  - name: payment_type
    type: int64
    description: "Payment type ID"
  - name: fare_amount
    type: float64
    description: "Fare amount in USD"
  - name: extra
    type: float64
    description: "Extra charges in USD"
  - name: mta_tax
    type: float64
    description: "MTA tax in USD"
  - name: tip_amount
    type: float64
    description: "Tip amount in USD"
  - name: tolls_amount
    type: float64
    description: "Tolls in USD"
  - name: total_amount
    type: float64
    description: "Total fare in USD"
  - name: extracted_at
    type: timestamp
    description: "Timestamp when data was extracted"

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python

import os
import json
import pandas as pd
import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def materialize():
    """
    Fetch NYC Taxi trip data from TLC public endpoint for the requested period and taxi types.

    Uses Bruin runtime context:
    - BRUIN_START_DATE / BRUIN_END_DATE: Date range to fetch (YYYY-MM-DD)
    - BRUIN_VARS: Pipeline variables (JSON), including taxi_types array

    Returns:
    - DataFrame with raw trip data + extracted_at timestamp
    - Uses append strategy: no deduplication here (handled in staging)
    """
    # Read environment variables
    start_date_str = os.getenv('BRUIN_START_DATE')
    end_date_str = os.getenv('BRUIN_END_DATE')
    bruin_vars = json.loads(os.getenv('BRUIN_VARS', '{}'))
    
    # Parse dates
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d') if start_date_str else datetime(2024, 1, 1)
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d') if end_date_str else datetime.now()
    
    # Get taxi types from pipeline variables (default: yellow, green)
    taxi_types = bruin_vars.get('taxi_types', ['yellow', 'green'])
    
    # TLC endpoint base URL
    base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
    
    # Generate list of (year, month, taxi_type) tuples to fetch
    current = start_date
    fetch_list = []
    
    while current <= end_date:
        year = current.year
        month = current.month
        for taxi_type in taxi_types:
            fetch_list.append((year, month, taxi_type))
        current = current + relativedelta(months=1)
    
    # Fetch data for each (year, month, taxi_type)
    dataframes = []
    extraction_timestamp = datetime.utcnow()
    
    for year, month, taxi_type in fetch_list:
        # Construct filename and URL
        filename = f'{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet'
        url = f'{base_url}{filename}'
        
        try:
            print(f'Fetching {url}...')
            # Download and read parquet file
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                # Read parquet from bytes
                df = pd.read_parquet(pd.io.common.BytesIO(response.content))
                # Add extraction metadata
                df['extracted_at'] = extraction_timestamp
                dataframes.append(df)
                print(f'  ✓ Loaded {len(df)} rows')
            else:
                print(f'  ✗ Failed: HTTP {response.status_code} (file may not exist)')
        except Exception as e:
            print(f'  ✗ Error fetching {filename}: {str(e)}')
    
    # Concatenate all DataFrames
    if not dataframes:
        # Return empty DataFrame with expected schema if no data was fetched
        return pd.DataFrame(columns=[
            'VendorID', 'pickup_datetime', 'dropoff_datetime', 'passenger_count',
            'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID',
            'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax',
            'tip_amount', 'tolls_amount', 'total_amount', 'extracted_at'
        ])
    
    final_df = pd.concat(dataframes, ignore_index=True)
    
    print(f'\nTotal rows fetched: {len(final_df)}')
    return final_df


