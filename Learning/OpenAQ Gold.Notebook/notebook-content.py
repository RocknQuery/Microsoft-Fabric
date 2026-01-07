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

df_silver = spark.table("OpenAQ_monthly_silver")
display(df_silver)

df_gold = (
    df_silver
    .filter(F.col("metric_name").isin("pm25", "pm1", "pm10"))
    .filter(F.col("percentComplete") >= 75)
    .groupBy("year", "month", "metric_name")
    .agg(
        F.countDistinct("sensor_id").alias("sensor_cnt"),
        F.min(F.when(F.col("min_value") < 0, 0).otherwise(F.col("min_value"))).alias("min_value_city"),
        F.max("max_value").alias("max_value_city"),
        (
            F.sum(F.col("avg_value") * F.col("observedCount")) /
            F.sum(F.col("observedCount"))
        ).alias("avg_value_city")
    )
    .orderBy("year","month","metric_name")
)
display(df_gold)
df_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("OpenAQ_monthly_gold")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
