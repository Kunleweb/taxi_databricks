# Databricks notebook source
# MAGIC %run ../bootstrap

# COMMAND ----------

from modules.utils.date_utils import get_month_start_n_months_ago
from modules.config import YELLOW_TRIPS_CLEANSED, YELLOW_TRIPS_ENRICHED, TAXI_ZONE_LOOKUP

# COMMAND ----------

two_months_ago_start = get_month_start_n_months_ago(2)
one_month_ago_start = get_month_start_n_months_ago(1)

# COMMAND ----------

df_trips = spark.read.table(YELLOW_TRIPS_CLEANSED).filter(
    f"tpep_pickup_datetime >= '{two_months_ago_start}' AND tpep_pickup_datetime < '{one_month_ago_start}'"
)

df_zones = spark.read.table(TAXI_ZONE_LOOKUP)

# COMMAND ----------

df_join_1 = df_trips.join(
    df_zones,
    df_trips.pu_location_id == df_zones.location_id,
    "left",
).select(
    df_trips.vendor,
    df_trips.tpep_pickup_datetime,
    df_trips.tpep_dropoff_datetime,
    df_trips.trip_duration,
    df_trips.passenger_count,
    df_trips.trip_distance,
    df_trips.rate_type,
    df_zones.borough.alias("pu_borough"),
    df_zones.zone.alias("pu_zone"),
    df_trips.do_location_id,
    df_trips.payment_type,
    df_trips.fare_amount,
    df_trips.extra,
    df_trips.mta_tax,
    df_trips.tolls_amount,
    df_trips.improvement_surcharge,
    df_trips.total_amount,
    df_trips.congestion_surcharge,
    df_trips.airport_fee,
    df_trips.cbd_congestion_fee,
    df_trips.processed_timestamp,
)

# COMMAND ----------

df_join_final = df_join_1.join(
    df_zones,
    df_join_1.do_location_id == df_zones.location_id,
    "left",
).select(
    df_join_1.vendor,
    df_join_1.tpep_pickup_datetime,
    df_join_1.tpep_dropoff_datetime,
    df_join_1.trip_duration,
    df_join_1.passenger_count,
    df_join_1.trip_distance,
    df_join_1.rate_type,
    df_join_1.pu_borough,
    df_zones.borough.alias("do_borough"),
    df_join_1.pu_zone,
    df_zones.zone.alias("do_zone"),
    df_join_1.payment_type,
    df_join_1.fare_amount,
    df_join_1.extra,
    df_join_1.mta_tax,
    df_join_1.tolls_amount,
    df_join_1.improvement_surcharge,
    df_join_1.total_amount,
    df_join_1.congestion_surcharge,
    df_join_1.airport_fee,
    df_join_1.cbd_congestion_fee,
    df_join_1.processed_timestamp,
)

# COMMAND ----------

df_join_final.write.mode("append").saveAsTable(YELLOW_TRIPS_ENRICHED)
