# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "26ab4736-9299-4ffd-885b-8b13b5cf2af9",
# META       "default_lakehouse_name": "TLC_Trip_Lakehouse",
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

df = spark.table("GDP_USA_Bronze")
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("gdp_usa_silver")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
