# Databricks notebook source
# MAGIC %run ../bootstrap

# COMMAND ----------

from pyspark.sql.functions import col, when, timestamp_diff
from modules.utils.date_utils import get_month_start_n_months_ago
from modules.config import YELLOW_TRIPS_RAW, YELLOW_TRIPS_CLEANSED

# COMMAND ----------

two_months_ago_start = get_month_start_n_months_ago(2)
one_month_ago_start = get_month_start_n_months_ago(1)

# COMMAND ----------

df = spark.read.table(YELLOW_TRIPS_RAW).filter(
    f"tpep_pickup_datetime >= '{two_months_ago_start}' AND tpep_pickup_datetime < '{one_month_ago_start}'"
)

# COMMAND ----------

df = df.select(
    when(col("VendorID") == 1, "Creative Mobile Technologies, LLC")
      .when(col("VendorID") == 2, "Curb Mobility, LLC")
      .when(col("VendorID") == 6, "Myle Technologies Inc")
      .when(col("VendorID") == 7, "Helix")
      .otherwise("Unknown")
      .alias("vendor"),

    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    timestamp_diff("MINUTE", col("tpep_pickup_datetime"), col("tpep_dropoff_datetime")).alias("trip_duration"),
    "passenger_count",
    "trip_distance",

    when(col("RatecodeID") == 1, "Standard Rate")
      .when(col("RatecodeID") == 2, "JFK")
      .when(col("RatecodeID") == 3, "Newark")
      .when(col("RatecodeID") == 4, "Nassau or Westchester")
      .when(col("RatecodeID") == 5, "Negotiated Fare")
      .when(col("RatecodeID") == 6, "Group Ride")
      .otherwise("Unknown")
      .alias("rate_type"),

    "store_and_fwd_flag",
    col("PULocationID").alias("pu_location_id"),
    col("DOLocationID").alias("do_location_id"),

    when(col("payment_type") == 0, "Flex Fare trip")
      .when(col("payment_type") == 1, "Credit card")
      .when(col("payment_type") == 2, "Cash")
      .when(col("payment_type") == 3, "No charge")
      .when(col("payment_type") == 4, "Dispute")
      .when(col("payment_type") == 6, "Voided trip")
      .otherwise("Unknown")
      .alias("payment_type"),

    "fare_amount",
    "extra",
    "mta_tax",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    col("Airport_fee").alias("airport_fee"),
    "cbd_congestion_fee",
    "processed_timestamp",
)

# COMMAND ----------

df.write.mode("append").saveAsTable(YELLOW_TRIPS_CLEANSED)
