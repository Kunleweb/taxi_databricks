# Databricks notebook source
# MAGIC %run ../bootstrap

# COMMAND ----------

from modules.utils.date_utils import get_target_yyyymm
from modules.data_loader.file_downloader import download_file
from modules.config import YELLOW_TAXI_VOLUME_PATH, YELLOW_TAXI_SOURCE_URL

# COMMAND ----------

formatted_date = get_target_yyyymm(2)

dir_path = f"{YELLOW_TAXI_VOLUME_PATH}/{formatted_date}"
local_path = f"{dir_path}/yellow_tripdata_{formatted_date}.parquet"

try:
    dbutils.fs.ls(local_path)
    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print("File already downloaded, aborting downstream tasks")
except:
    try:
        url = YELLOW_TAXI_SOURCE_URL.format(yyyymm=formatted_date)
        download_file(url, dir_path, local_path)
        dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
        print("File successfully uploaded in current run")
    except Exception as e:
        dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
        print(f"File download failed: {str(e)}")
