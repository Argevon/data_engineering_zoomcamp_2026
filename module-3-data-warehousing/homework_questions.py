import os
from google.cloud import bigquery

# Set up credentials
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
client = bigquery.Client()

# Configuration - Replace with your actual values
PROJECT_ID = "your-gcp-project-id"
DATASET_ID = "your_dataset_name"
REGULAR_TABLE = f"{PROJECT_ID}.{DATASET_ID}.yellow_trips"
PARTITIONED_TABLE = f"{PROJECT_ID}.{DATASET_ID}.yellow_trips_optimized"


print("=" * 80)
print("YELLOW TAXI DATA ANALYSIS HOMEWORK")
print("=" * 80)

# Question 1: Counting total records
print("\nQuestion 1: Counting records")
print("-" * 80)
query1 = f"""
SELECT COUNT(*) as total_records
FROM `{REGULAR_TABLE}`
"""
# Get estimate first
job_dry = client.query(query1, job_config=bigquery.QueryJobConfig(dry_run=True))
print(f"Estimated bytes: {job_dry.total_bytes_processed / (1024**2):.2f} MB")

# Run actual query
job = client.query(query1)
result = job.result()
for row in result:
    total_records = row.total_records
    print(f"Total records in dataset: {total_records:,}")

# Question 4: Counting zero fare trips
print("\nQuestion 4: Zero fare trips")
print("-" * 80)
query4 = f"""
SELECT COUNT(*) as zero_fare_count
FROM `{REGULAR_TABLE}`
WHERE fare_amount = 0
"""
# Get estimate first
job4_dry = client.query(query4, job_config=bigquery.QueryJobConfig(dry_run=True))
print(f"Estimated bytes: {job4_dry.total_bytes_processed / (1024**2):.2f} MB")

# Run actual query
job4 = client.query(query4)
result4 = job4.result()
for row in result4:
    zero_count = row.zero_fare_count
    print(f"Trips with fare_amount = 0: {zero_count:,}")

# Question 5: Create partitioned and clustered table
print("\nQuestion 5: Creating optimized table")
print("-" * 80)
print("Creating table with:")
print("  - Partition by: DATE(tpep_dropoff_datetime)")
print("  - Cluster by: VendorID")

query5 = f"""
CREATE OR REPLACE TABLE `{PARTITIONED_TABLE}`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT *
FROM `{REGULAR_TABLE}`
"""

try:
    job5 = client.query(query5)
    job5.result()
    print("Table created successfully")
except Exception as e:
    print(f"Error creating table: {e}")

# Question 6: Comparing partition benefits
print("\nQuestion 6: Partition benefits comparison")
print("-" * 80)

# Query on non-partitioned table
print("Query on regular (non-partitioned) table:")
query6a = f"""
SELECT DISTINCT VendorID
FROM `{REGULAR_TABLE}`
WHERE tpep_dropoff_datetime >= '2024-03-01' 
  AND tpep_dropoff_datetime <= '2024-03-15 23:59:59'
"""
# Get estimate
job6a_dry = client.query(query6a, job_config=bigquery.QueryJobConfig(dry_run=True))
bytes_non_partitioned = job6a_dry.total_bytes_processed / (1024**2)
print(f"Estimated bytes: {bytes_non_partitioned:.2f} MB")

# Run actual query
job6a = client.query(query6a)
result6a = job6a.result()
vendor_ids = [row.VendorID for row in result6a]
print(f"VendorIDs found: {vendor_ids}")

# Query on partitioned table
print("\nQuery on partitioned table:")
query6b = f"""
SELECT DISTINCT VendorID
FROM `{PARTITIONED_TABLE}`
WHERE tpep_dropoff_datetime >= '2024-03-01' 
  AND tpep_dropoff_datetime <= '2024-03-15 23:59:59'
"""
# Get estimate
job6b_dry = client.query(query6b, job_config=bigquery.QueryJobConfig(dry_run=True))
bytes_partitioned = job6b_dry.total_bytes_processed / (1024**2)
print(f"Estimated bytes: {bytes_partitioned:.2f} MB")

# Run actual query
job6b = client.query(query6b)
result6b = job6b.result()
vendor_ids_part = [row.VendorID for row in result6b]
print(f"VendorIDs found: {vendor_ids_part}")

# Question 7: External table storage location
print("\nQuestion 7: External table storage location")
print("-" * 80)
print("Data location for external tables:")
print("External table data is stored in: GCP Bucket")
print("BigQuery maintains metadata references to GCS files")
print("No data is copied into BigQuery storage")

# Question 8: Clustering best practices
print("\nQuestion 8: Best practice to always cluster")
print("-" * 80)
print("Answer: False")
print("\nWhen clustering is beneficial:")
print("  - Tables larger than 100GB")
print("  - Queries frequently filter on specific columns")
print("  - Queries commonly sort by the clustered columns")
print("\nWhen clustering is not recommended:")
print("  - Small tables (less than 10GB)")
print("  - Queries that scan the entire table")
print("  - Frequently changing filter patterns")

# Question 9: COUNT(*) bytes estimation explanation
print("\nQuestion 9: SELECT COUNT(*) bytes estimation")
print("-" * 80)
query9 = f"""
SELECT COUNT(*) as total_rows
FROM `{REGULAR_TABLE}`
"""
# Get estimate
job9_dry = client.query(query9, job_config=bigquery.QueryJobConfig(dry_run=True))
bytes_estimated = job9_dry.total_bytes_processed / (1024**2)
print(f"Estimated bytes: {bytes_estimated:.2f} MB")

# Run actual query
job9 = client.query(query9)
result9 = job9.result()
for row in result9:
    row_count = row.total_rows
    print(f"Total rows: {row_count:,}")

print("\nWhy COUNT(*) scans all data:")
print("COUNT(*) requires reading the entire table to count all rows.")
print("BigQuery still needs to process every row, even though we're not")
print("selecting any specific columns. The materialized table stores all")
print("columns in storage, so the full table must be scanned.")

print("\n" + "=" * 80)
print("Analysis complete")
print("=" * 80)

