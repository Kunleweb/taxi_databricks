# Databricks notebook source
# MAGIC %run ../bootstrap

# COMMAND ----------

from pyspark.sql.functions import count, max, min, avg, sum, round
from modules.utils.date_utils import get_month_start_n_months_ago
from modules.config import YELLOW_TRIPS_ENRICHED, DAILY_TRIP_SUMMARY

# COMMAND ----------

two_months_ago_start = get_month_start_n_months_ago(2)
one_month_ago_start = get_month_start_n_months_ago(1)

# COMMAND ----------

df = spark.read.table(YELLOW_TRIPS_ENRICHED).filter(
    f"tpep_pickup_datetime >= '{two_months_ago_start}' AND tpep_pickup_datetime < '{one_month_ago_start}'"
)

# COMMAND ----------

df = df.groupBy(df.tpep_pickup_datetime.cast("date").alias("pickup_date")).agg(
    count("*").alias("total_trips"),
    round(avg("passenger_count"), 1).alias("average_passengers"),
    round(avg("trip_distance"), 1).alias("average_distance"),
    round(avg("fare_amount"), 2).alias("average_fare_per_trip"),
    max("fare_amount").alias("max_fare"),
    min("fare_amount").alias("min_fare"),
    round(sum("total_amount"), 2).alias("total_revenue"),
)

# COMMAND ----------

df.write.mode("append").saveAsTable(DAILY_TRIP_SUMMARY)
