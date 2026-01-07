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

df_silver = spark.table("silver_tlc_trips")

df_gold = (
    df_silver
    .withColumn("trip_year", F.year(F.col("pickup_date")))
    .withColumn("trip_month", F.month(F.col("pickup_date")))
    .groupBy("trip_year", "trip_month")
    .agg(
        F.count("*").alias("trips_cnt"),
        F.sum("fare_amount").alias("fare_sum"),
        F.sum("tip_amount").alias("tip_sum"),
        F.sum("total_amount").alias("total_sum"),
        F.sum("trip_distance").alias("distance_sum"),
        F.sum(F.when(F.col("tip_amount") > 0, 1).otherwise(0)).alias("trips_with_tips_cnt")
    )
    .filter(F.col("trips_cnt") >= 2000000)
    .orderBy(["trip_year", "trip_month"])
)
display(df_gold)

df_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("tlc_trips_gold")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
