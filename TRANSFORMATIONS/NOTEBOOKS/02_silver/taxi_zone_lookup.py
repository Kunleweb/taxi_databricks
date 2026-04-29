# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import TimestampType, IntegerType 
from datetime import datetime
from delta.tables import DeltaTable 

# COMMAND ----------

df = spark.read.format('csv').option("header", True).load("/Volumes/nyctaxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv")

# COMMAND ----------

df = df.select(
    col("LocationID").cast(IntegerType()).alias('location_id'),
    col("Borough").alias("borough"),
    col("Zone").alias("zone"),
    col("service_zone"), current_timestamp().alias('effective_date'), 
    lit(None).cast(TimestampType()).alias("end_date"))

# COMMAND ----------

#fixed point in time used to close any changed active records, using a python timestamp ensures the exact same value is written 
end_timestamp = datetime.now()

#load the SCD2 deltatable instance 
dt = DeltaTable.forName(spark, "nyctaxi.`02_silver`.taxi_zone_lookup")

# COMMAND ----------

#pass 1: close any active rows whose tracked attributed changed 
#match only the active target rows whos end_Date is null with the same business key
#if any tracked column differs, set end_Date to end_timestamp to retire that version

dt.alias('t').merge(
    source=df.alias('s'),
    condition = "t.location_id = s.location_id AND t.end_date is NULL and (t.borough !=s.borough or t.zone!=s.zone or t.service_zone != s.service_zone)"  
).whenMatchedUpdate(
    set={
        "t.end_date": lit(end_timestamp).cast(TimestampType())
    }
).execute()



# COMMAND ----------


# -----------------------------
# PASS 2: Insert new current versions
# -----------------------------
# Now insert a row for:
#   (a) keys we just closed in PASS 1 (no longer an active match), and
#   (b) brand-new keys not present in the target.
# We again match on *active* rows; anything without an active match is inserted.

# get the lists of IDs that have been closed
insert_id_list = [row.location_id for row in dt.toDF().filter(f"end_date = '{end_timestamp}' ").select("location_id").collect()]

# If the list is empty, don't try to insert anything
if len(insert_id_list) == 0:
    print("No updated records to insert")
else:
    dt.alias("t").\
        merge(
            source    = df.alias("s"),
            condition = f"s.location_id not in ({', '.join(map(str, insert_id_list))})"
        ).\
        whenNotMatchedInsert(
            values = { "t.location_id": "s.location_id",
                    "t.borough": "s.borough",
                    "t.zone": "s.zone",
                    "t.service_zone": "s.service_zone",
                    "t.effective_date": current_timestamp(),
                    "t.end_date": lit(None).cast(TimestampType()) }
        ).\
        execute()

# COMMAND ----------

#pass3: insert brand-new keys (no historical row in target)
dt.alias('t').merge(
    source=df.alias('s'),
    condition= "t.location_id=s.location_id"
).whenNotMatchedInsert(
    values={
                "t.location_id": "s.location_id",
                "t.borough": "s.borough",
                "t.zone":"s.zone",
                "t.service_zone":"s.service_zone",
                "t.effective_date": current_timestamp(),
                "t.end_date": lit(None).cast(TimestampType())
    }
).execute()

# COMMAND ----------

df.write.mode('overwrite').saveAsTable('nyctaxi.02_silver.taxi_zone_lookup')

# COMMAND ----------

