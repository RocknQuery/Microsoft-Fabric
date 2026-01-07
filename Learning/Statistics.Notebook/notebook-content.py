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

# MAGIC %%sql
# MAGIC create or replace table statistics as
# MAGIC with openaq_monthly_pivot as (
# MAGIC     select year,
# MAGIC            month,
# MAGIC            max(case when metric_name = 'pm25' then avg_value_city end) as pm25_avg,
# MAGIC            max(case when metric_name = 'pm10' then avg_value_city end) as pm10_avg,
# MAGIC            max(case when metric_name = 'pm1' then avg_value_city end) as pm1_avg,
# MAGIC            max(case when metric_name = 'pm25' then sensor_cnt end) as pm25_sensors,
# MAGIC            max(case when metric_name = 'pm10' then sensor_cnt end) as pm10_sensors,
# MAGIC            max(case when metric_name = 'pm1' then sensor_cnt end) as pm1_sensors
# MAGIC       from openaq_monthly_gold
# MAGIC      group by year, month
# MAGIC )
# MAGIC select tt.trip_year as year,
# MAGIC        tt.trip_month as month,
# MAGIC        tt.trips_cnt,
# MAGIC        tt.fare_sum,
# MAGIC        tt.tip_sum,
# MAGIC        tt.total_sum,
# MAGIC        tt.distance_sum,
# MAGIC        tt.trips_with_tips_cnt,
# MAGIC        om.pm25_sensors,
# MAGIC        om.pm25_avg,
# MAGIC        om.pm10_sensors,
# MAGIC        om.pm10_avg,
# MAGIC        om.pm1_sensors,
# MAGIC        om.pm1_avg,
# MAGIC        ue.obs_value as usd_eur_rate,
# MAGIC        gu.value as yearly_gdp
# MAGIC   from tlc_trips_gold tt
# MAGIC   left join openaq_monthly_pivot om on om.year = tt.trip_year and om.month = tt.trip_month
# MAGIC   left join usd_eur_rates_gold ue on ue.year = tt.trip_year and ue.month = tt.trip_month
# MAGIC   left join gdp_usa_gold gu on gu.date = tt.trip_year

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
