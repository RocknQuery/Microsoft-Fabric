# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "26ab4736-9299-4ffd-885b-8b13b5cf2af9",
# META       "default_lakehouse_name": "Lakehouse",
# META       "default_lakehouse_workspace_id": "ddfee769-f6e9-4ae9-b14e-55757d57d7ba",
# META       "known_lakehouses": [
# META         {
# META           "id": "26ab4736-9299-4ffd-885b-8b13b5cf2af9"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
import pyspark.sql.types

desired_schema_green = StructType([
    StructField("VendorID", types.LongType(), True),
    StructField("pickup_datetime", types.TimestampNTZType(), True),
    StructField("dropoff_datetime", types.TimestampNTZType(), True),
    StructField("store_and_fwd_flag", types.StringType(), True),
    StructField("RateCodeID", types.DoubleType(), True),
    StructField("PULocationID", types.LongType(), True),
    StructField("DOLocationID", types.LongType(), True),
    StructField("passenger_count", types.DoubleType(), True),
    StructField("trip_distance", types.DoubleType(), True),
    StructField("fare_amount", types.DoubleType(), True),
    StructField("extra", types.DoubleType(), True),
    StructField("mta_tax", types.DoubleType(), True),
    StructField("tip_amount", types.DoubleType(), True),
    StructField("tolls_amount", types.DoubleType(), True),
    StructField("ehail_fee", types.DoubleType(), True),
    StructField("improvement_surcharge", types.DoubleType(), True),
    StructField("total_amount", types.DoubleType(), True),
    StructField("payment_type", types.DoubleType(), True),
    StructField("trip_type", types.DoubleType(), True),
    StructField("congestion_surcharge", types.DoubleType(), True),
])

yellow_df = spark.read.parquet("Files/bronze/tlc/yellow/*/*/*")
green_df = spark.read.schema(desired_schema_green).parquet("Files/bronze/tlc/green/2023/*/*")
yellow_df = yellow_df.withColumn("taxi_type", F.lit("yellow"))
green_df = green_df.withColumn("taxi_type", F.lit("green"))

yellow_df = (
    yellow_df
    .withColumn("VendorID", F.col("VendorID").cast(types.LongType()))
    .withColumn("RateCodeID", F.col("RateCodeID").cast(types.LongType()))
    .withColumn("passenger_count", F.col("passenger_count").cast(types.LongType()))
    .withColumn("PULocationID", F.col("PULocationID").cast(types.LongType()))
    .withColumn("DOLocationID", F.col("DOLocationID").cast(types.LongType()))
    .withColumn("payment_type", F.col("payment_type").cast(types.LongType()))
    .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    .withColumn("ehail_fee", F.lit(None).cast(types.IntegerType()))
    .withColumn("trip_type", F.lit(None).cast(types.DoubleType()))
)

green_df = (
    green_df
    .withColumn("VendorID", F.col("VendorID").cast(types.LongType()))
    .withColumn("passenger_count", F.col("passenger_count").cast(types.LongType()))
    .withColumn("RateCodeID", F.col("RateCodeID").cast(types.LongType()))
    .withColumn("payment_type", F.col("payment_type").cast(types.LongType()))
    .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
    .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
    .withColumn("Airport_fee", F.lit(None).cast(types.DoubleType()))
)

# merged_df = yellow_df.unionByName(green_df, allowMissingColumns=True)
yellow_df.printSchema()
green_df.printSchema()
yellow_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable("tlc_trips_bronze_yellow")
green_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable("tlc_trips_bronze_green")
# merged_df = yellow_df.unionByName(green_df, allowMissingColumns=True)
# merged_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable("tlc_trips_bronze")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
