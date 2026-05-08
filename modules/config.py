CATALOG = "nyctaxi"
SCHEMA_LANDING = "00_landing"
SCHEMA_BRONZE = "01_bronze"
SCHEMA_SILVER = "02_silver"
SCHEMA_GOLD = "03_gold"

VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA_LANDING}/data_sources"
YELLOW_TAXI_VOLUME_PATH = f"{VOLUME_ROOT}/nyctaxi_yellow"
ZONE_LOOKUP_VOLUME_PATH = f"{VOLUME_ROOT}/lookup"

# Source URLs — use .format(yyyymm=...) for the taxi URL
YELLOW_TAXI_SOURCE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{yyyymm}.parquet"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# Fully-qualified Delta table names
YELLOW_TRIPS_RAW = f"{CATALOG}.{SCHEMA_BRONZE}.yellow_trips_raw"
YELLOW_TRIPS_CLEANSED = f"{CATALOG}.{SCHEMA_SILVER}.yellow_trips_cleansed"
YELLOW_TRIPS_ENRICHED = f"{CATALOG}.{SCHEMA_SILVER}.yellow_trips_enriched"
TAXI_ZONE_LOOKUP = f"{CATALOG}.{SCHEMA_SILVER}.taxi_zone_lookup"
DAILY_TRIP_SUMMARY = f"{CATALOG}.{SCHEMA_GOLD}.daily_trip_summary"
