# Databricks notebook source
# MAGIC %run ../bootstrap

# COMMAND ----------

from modules.utils.date_utils import get_target_yyyymm
from modules.transformations.metadata import add_processed_timestamp
from modules.config import YELLOW_TAXI_VOLUME_PATH, YELLOW_TRIPS_RAW

# COMMAND ----------

formatted_date = get_target_yyyymm(2)

df = spark.read.format("parquet").load(f"{YELLOW_TAXI_VOLUME_PATH}/{formatted_date}")

# COMMAND ----------

df = add_processed_timestamp(df)

# COMMAND ----------

df.write.mode("append").saveAsTable(YELLOW_TRIPS_RAW)
