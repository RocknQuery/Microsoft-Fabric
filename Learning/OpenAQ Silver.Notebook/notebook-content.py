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

bronze_df = spark.table("OpenAQ_monthly_bronze")
display(bronze_df)
silver_df = (
    bronze_df
    .withColumnRenamed("sensorId", "sensor_id")
    .withColumn("year", F.year("utc_from"))
    .withColumn("month", F.month("utc_from"))
    .filter(F.col("value").isNotNull())
    .select(
        "sensor_id",
        "year",
        "month",
        "metric_name",
        "metric_units",
        "value",
        F.col("`summary.min`").alias("min_value"),
        F.col("`summary.max`").alias("max_value"),
        F.col("`summary.avg`").alias("avg_value"),
        "expectedCount",
        "observedCount",
        "percentComplete",
        "percentCoverage"
    )
)
silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("OpenAQ_monthly_silver")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
