from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName("check").getOrCreate()
print(spark.version)
spark.stop()