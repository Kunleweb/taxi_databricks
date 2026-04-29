# Databricks notebook source
from pyspark.sql.functions import count, max, min, round, avg,col,sum

# COMMAND ----------

df1= spark.read.table('nyctaxi.`02_silver`.yellow_trips_enriched')
df2 = spark.read.table('nyctaxi.`03_gold`.daily_trip_summary')

# COMMAND ----------

# MAGIC %md
# MAGIC Which vendor makes the most revenue?

# COMMAND ----------


df1.groupBy('vendor').agg(sum('total_amount').alias('total_revenue')).orderBy('total_revenue', ascending=False).display()

# COMMAND ----------

# MAGIC %md
# MAGIC What is the most popular pickup brough?

# COMMAND ----------

df1.groupBy('pu_borough').agg(count('*').alias('number_of_trips')).orderBy('number_of_trips', ascending=False).display()

# COMMAND ----------

# MAGIC %md
# MAGIC What is the most common journey (borough to borough) ?

# COMMAND ----------

df1.groupBy('pu_borough', 'do_borough').agg(count('*').alias('number_of_trips')).orderBy('number_of_trips', ascending=False).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Create a time series chart showing the number of trips and total revenue per day?

# COMMAND ----------

df2.display()