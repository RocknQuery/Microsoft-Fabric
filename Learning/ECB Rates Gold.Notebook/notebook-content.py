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
from pyspark.sql.window import Window

df_silver = (
    spark.table("usd_eur_rates_silver")
    .select("CURRENCY", "CURRENCY_DENOM", "TIME_PERIOD", "OBS_VALUE")
    .withColumnRenamed("TIME_PERIOD", "rate_date")
    .filter(F.col("OBS_VALUE").isNotNull())
)

w = Window.partitionBy(
    "CURRENCY", "CURRENCY_DENOM", F.year("rate_date"), F.month("rate_date")
).orderBy(F.col("rate_date").desc())

df_gold = (
    df_silver
    .withColumn("year", F.year("rate_date"))
    .withColumn("month", F.month("rate_date"))
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "year", "month", "CURRENCY", "CURRENCY_DENOM",
        F.col("rate_date"),
        F.col("OBS_VALUE")
    )
    .orderBy("year", "month")
)

display(df_gold)
df_gold.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("usd_eur_rates_gold")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
