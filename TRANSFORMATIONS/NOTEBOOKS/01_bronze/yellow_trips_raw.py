# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from datetime import datetime
from datetime import date, datetime, timezone
from dateutil.relativedelta import relativedelta


# COMMAND ----------

#obtain the year-month for 2 months prior to the current month in yy-mm format
two_month_ago = date.today() - relativedelta(months=2)
formatted_date = two_month_ago.strftime("%Y-%m")
df = spark.read.format('parquet').load(f'/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{formatted_date}')

# COMMAND ----------

df = df.withColumn("processed_timestamp", current_timestamp())

# COMMAND ----------

df.write.mode('overwrite').saveAsTable('nyctaxi.01_bronze.yellow_trips_raw')