import os
from google.cloud import bigquery

# Set up credentials
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
client = bigquery.Client()

# Configuration
PROJECT_ID = "your-gcp-project-id"  # Change to your actual project ID
DATASET_ID = "your_dataset_name"     # Change to your dataset name
GCS_BUCKET = "your-bucket-name"      # Change to your GCS bucket name
EXTERNAL_TABLE_ID = "yellow_trips_external"
REGULAR_TABLE_ID = "yellow_trips"


def create_dataset():
    """Create BigQuery dataset if it doesn't exist"""
    dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    
    try:
        dataset = client.create_dataset(dataset, exists_ok=True)
        print(f"✓ Dataset {dataset_id} created or already exists")
    except Exception as e:
        print(f"✗ Error creating dataset: {e}")
        return False
    return True


def create_external_table():
    """Create external table pointing to GCS parquet files"""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{EXTERNAL_TABLE_ID}"
    
    # GCS path pattern for all months (01-06)
    gcs_uris = [
        f"gs://{GCS_BUCKET}/yellow_tripdata_2024-*.parquet"
    ]
    
    external_config = bigquery.ExternalConfig(bigquery.SourceFormat.PARQUET)
    external_config.source_uris = gcs_uris
    external_config.autodetect = True  # Auto-detect schema
    
    table = bigquery.Table(table_id)
    table.external_data_configuration = external_config
    
    try:
        table = client.create_table(table, exists_ok=True)
        print(f"✓ External table {table_id} created")
        return True
    except Exception as e:
        print(f"✗ Error creating external table: {e}")
        return False


def create_regular_table():
    """Create regular (materialized) table from external table"""
    external_table_id = f"{PROJECT_ID}.{DATASET_ID}.{EXTERNAL_TABLE_ID}"
    regular_table_id = f"{PROJECT_ID}.{DATASET_ID}.{REGULAR_TABLE_ID}"
    
    # SQL to create regular table from external table
    query = f"""
    CREATE OR REPLACE TABLE `{regular_table_id}` AS
    SELECT *
    FROM `{external_table_id}`
    """
    
    try:
        query_job = client.query(query)
        query_job.result()  # Wait for query to complete
        print(f"✓ Regular table {regular_table_id} created")
        return True
    except Exception as e:
        print(f"✗ Error creating regular table: {e}")
        return False


def describe_tables():
    """Print table schemas"""
    try:
        # External table
        external_table = client.get_table(f"{PROJECT_ID}.{DATASET_ID}.{EXTERNAL_TABLE_ID}")
        print(f"\nExternal table schema ({EXTERNAL_TABLE_ID}):")
        print(external_table.schema)
        
        # Regular table
        regular_table = client.get_table(f"{PROJECT_ID}.{DATASET_ID}.{REGULAR_TABLE_ID}")
        print(f"\nRegular table schema ({REGULAR_TABLE_ID}):")
        print(regular_table.schema)
        
        # Row counts
        count_query = f"SELECT COUNT(*) as row_count FROM `{PROJECT_ID}.{DATASET_ID}.{REGULAR_TABLE_ID}`"
        count_job = client.query(count_query)
        row_count = count_job.result().to_dataframe().iloc[0]['row_count']
        print(f"\nTotal rows in {REGULAR_TABLE_ID}: {row_count:,}")
        
    except Exception as e:
        print(f"✗ Error describing tables: {e}")


if __name__ == "__main__":
    print("=" * 60)
    print("Creating BigQuery Tables from Yellow Taxi Data")
    print("=" * 60)
    
    # Step 1: Create dataset
    if create_dataset():
        # Step 2: Create external table
        if create_external_table():
            # Step 3: Create regular table
            if create_regular_table():
                # Step 4: Describe tables
                describe_tables()
                print("\n✓ All tables created successfully!")
            else:
                print("\n✗ Failed to create regular table")
        else:
            print("\n✗ Failed to create external table")
    else:
        print("\n✗ Failed to create dataset")
