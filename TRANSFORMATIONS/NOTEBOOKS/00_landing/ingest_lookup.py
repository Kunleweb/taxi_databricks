# Databricks notebook source
import shutil
import os
import urllib.request



try:

    #target url
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    #response 
    response = urllib.request.urlopen(url)
    #destination
    dir_path = "/Volumes/nyctaxi/00_landing/data_sources/lookup"
    os.makedirs(dir_path, exist_ok= True)

    #define the full local path (including the filename) where the file will be saved
    local_path = "/Volumes/nyctaxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv"

    #write the content of response to the local file path
    with open(local_path, 'wb') as f:
        shutil.copyfileobj(response, f)
    
    dbutils.jobs.taskValues.set(key= "continue_downstream", value= "yes")
    print("Files successfully uploaded")
except  Exception as e:
    dbutils.job.taskValues.set(key="continue_downstream", value = "no")
    print(f"file download failed due to {str(e)}")