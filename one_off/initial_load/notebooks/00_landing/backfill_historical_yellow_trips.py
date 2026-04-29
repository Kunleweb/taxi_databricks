# Databricks notebook source
import urllib.request 
import shutil 
import os

# COMMAND ----------

dates_to_process = ['2025-09','2025-10','2025-11','2025-12','2026-01','2026-02']

for dates in dates_to_process:
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{dates}.parquet"
    response = urllib.request.urlopen(url)
    dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{dates}"
    os.makedirs(dir_path, exist_ok=True)
    local_path = dir_path + f"/yellow_tripdata_{dates}.parquet" 
    with open(local_path, 'wb') as  f:
        shutil.copyfileobj(response, f)


# COMMAND ----------

