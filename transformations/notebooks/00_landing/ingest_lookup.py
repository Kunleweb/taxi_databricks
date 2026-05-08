# Databricks notebook source
# MAGIC %run ../bootstrap

# COMMAND ----------

from modules.data_loader.file_downloader import download_file
from modules.config import ZONE_LOOKUP_VOLUME_PATH, ZONE_LOOKUP_URL

# COMMAND ----------

try:
    dir_path = ZONE_LOOKUP_VOLUME_PATH
    local_path = f"{dir_path}/taxi_zone_lookup.csv"
    download_file(ZONE_LOOKUP_URL, dir_path, local_path)
    dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
    print("File successfully uploaded")
except Exception as e:
    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print(f"File download failed: {str(e)}")
