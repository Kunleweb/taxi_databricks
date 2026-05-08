# Databricks notebook source
# MAGIC %run ../bootstrap

# COMMAND ----------

from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.functions import lit, current_timestamp, col
from pyspark.sql.types import TimestampType, IntegerType
from modules.config import ZONE_LOOKUP_VOLUME_PATH, TAXI_ZONE_LOOKUP

# COMMAND ----------

df = spark.read.format("csv").option("header", True).load(f"{ZONE_LOOKUP_VOLUME_PATH}/taxi_zone_lookup.csv")

# COMMAND ----------

df = df.select(
    col("LocationID").cast(IntegerType()).alias("location_id"),
    col("Borough").alias("borough"),
    col("Zone").alias("zone"),
    col("service_zone"),
    current_timestamp().alias("effective_date"),
    lit(None).cast(TimestampType()).alias("end_date"),
)

# COMMAND ----------

end_timestamp = datetime.now()

dt = DeltaTable.forName(spark, TAXI_ZONE_LOOKUP)

# COMMAND ----------

# PASS 1: Close any active rows whose tracked attributes changed
dt.alias("t").merge(
    source=df.alias("s"),
    condition=(
        "t.location_id = s.location_id AND t.end_date IS NULL "
        "AND (t.borough != s.borough OR t.zone != s.zone OR t.service_zone != s.service_zone)"
    ),
).whenMatchedUpdate(
    set={"t.end_date": lit(end_timestamp).cast(TimestampType())}
).execute()

# COMMAND ----------

# PASS 2: Insert new versions for records closed in PASS 1
insert_id_list = [
    row.location_id
    for row in dt.toDF().filter(f"end_date = '{end_timestamp}'").select("location_id").collect()
]

if len(insert_id_list) == 0:
    print("No updated records to insert")
else:
    dt.alias("t").merge(
        source=df.alias("s"),
        condition=f"s.location_id not in ({', '.join(map(str, insert_id_list))})",
    ).whenNotMatchedInsert(
        values={
            "t.location_id": "s.location_id",
            "t.borough": "s.borough",
            "t.zone": "s.zone",
            "t.service_zone": "s.service_zone",
            "t.effective_date": current_timestamp(),
            "t.end_date": lit(None).cast(TimestampType()),
        }
    ).execute()

# COMMAND ----------

# PASS 3: Insert brand-new keys (no historical row in target)
dt.alias("t").merge(
    source=df.alias("s"),
    condition="t.location_id = s.location_id",
).whenNotMatchedInsert(
    values={
        "t.location_id": "s.location_id",
        "t.borough": "s.borough",
        "t.zone": "s.zone",
        "t.service_zone": "s.service_zone",
        "t.effective_date": current_timestamp(),
        "t.end_date": lit(None).cast(TimestampType()),
    }
).execute()
