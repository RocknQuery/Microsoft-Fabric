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

def col_if_exists(name: str):
    return F.col(name) if name in merged_df.columns else F.lit(None)

yellow_df = spark.read.parquet("Files/bronze/tlc/yellow/*/*/*")
green_df = spark.read.parquet("Files/bronze/tlc/green/*/*/*")
yellow_df = yellow_df.withColumn("taxi_type", F.lit("yellow"))
green_df = green_df.withColumn("taxi_type", F.lit("green"))
merged_df = yellow_df.unionByName(green_df, allowMissingColumns=True)

pickup_ts = F.coalesce(
    col_if_exists("lpep_pickup_datetime"),
    col_if_exists("tpep_pickup_datetime")
)
dropoff_ts = F.coalesce(
    col_if_exists("lpep_dropoff_datetime"),
    col_if_exists("tpep_dropoff_datetime")
)
df_silver = (
    merged_df
    .withColumn("pickup_datetime", F.to_timestamp(pickup_ts))
    .withColumn("dropoff_datetime", F.to_timestamp(dropoff_ts))
    .withColumn("pickup_date", F.to_date(F.col("pickup_datetime")))
)
display(merged_df)
df_silver.write.format("delta").mode("append").saveAsTable("tlc_trips_silver")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
