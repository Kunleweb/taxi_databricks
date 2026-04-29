# Databricks notebook source
import urllib.request 
import shutil 
import os
from datetime import datetime
from datetime import date, datetime, timezone
from dateutil.relativedelta import relativedelta


#obtain the year-month for 2 months prior to the current month in yy-mm format
two_month_ago = date.today() - relativedelta(months=2)
formatted_date = two_month_ago.strftime("%Y-%m")

#define the local directloru for this dates data
dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{formatted_date}"

#define the full path for the downloaded file 
local_path = f"{dir_path}/yellow_tripdata_{formatted_date}.parquet"

try: 
    dbutils.fs.ls(local_path)
    dbutils.jobs.taskValues.set(key= "continue_downstream", value="no")
    print('file already downloaded, aborting downstream tasks')
except:
    try:
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{formatted_date}.parquet"
        response = urllib.request.urlopen(url)
        os.makedirs(dir_path, exist_ok=True)
        with open(local_path, 'wb') as f:
            shutil.copyfileobj(response, f)
        dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
        print("File successfuly uploaded in current run") 
    except Exception as e:
        dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
        print(f"file download failed: {str(e)}")
        
