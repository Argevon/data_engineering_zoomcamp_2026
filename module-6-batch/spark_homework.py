import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read.load("yellow_tripdata_2025-11.parquet")

# Q2: Repartition to 4 partitions and save to parquet check avg size
 
# df_repart = df.repartition(4)
# df_repart.write.mode("overwrite").parquet("yellow_tripdata_2025-11_repartitioned.parquet")

# Q3: Number of trips on 15th November 2025
from pyspark.sql.functions import col, expr
df_15th = df.filter(col("tpep_pickup_datetime").cast("date") == "2025-11-15")
num_trips_15th = df_15th.count()
print(f"Number of trips on 15th November 2025: {num_trips_15th}")

# Q4: What is the length of the longest trip in the dataset in hours?
from pyspark.sql.functions import max
df_with_duration = df.withColumn(
    "trip_duration_hours",
    expr("timestampdiff(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime) / 3600.0"),
)
longest_trip_duration = df_with_duration.agg(max("trip_duration_hours")).first()[0]
print(f"Length of the longest trip in hours: {longest_trip_duration}")

#Q6: Least frequent pickup location zone
from pyspark.sql.functions import count
df_taxi_zones = spark.read.load("taxi_zone_lookup.csv", format="csv", header=True)
df_joined = df.join(df_taxi_zones, df.PULocationID == df_taxi_zones.LocationID, "left")
pickup_zone_counts = df_joined.groupBy("Zone").agg(count("*").alias("pickup_count"))
least_frequent_zone = pickup_zone_counts.orderBy("pickup_count").first()
second_least_frequent_zone = pickup_zone_counts.orderBy("pickup_count").take(2)[1]
print(f"Least frequent pickup location zone: {least_frequent_zone['Zone']} with {least_frequent_zone['pickup_count']} pickups") 
print(f"Second least frequent pickup location zone: {second_least_frequent_zone['Zone']} with {second_least_frequent_zone['pickup_count']} pickups")