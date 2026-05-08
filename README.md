# taxi_databricks

An end-to-end data engineering pipeline that ingests, transforms, and analyses NYC Yellow Taxi trip data on Databricks using a Medallion Architecture (Bronze → Silver → Gold) with Unity Catalog.

---

## Architecture

```
NYC TLC (public Parquet/CSV)
        │
        ▼
┌─────────────────────┐
│  00_landing (Volume)│  Raw files on disk (Parquet / CSV)
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│  01_bronze          │  yellow_trips_raw — raw rows + processed_timestamp
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│  02_silver          │  yellow_trips_cleansed — decoded fields, trip duration
│                     │  yellow_trips_enriched — joined with taxi zone lookup
│                     │  taxi_zone_lookup      — SCD2 zone reference table
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│  03_gold            │  daily_trip_summary — daily KPI aggregates
└─────────────────────┘
```

The pipeline runs on a **2-month lag** — a job running in May processes March data. This accounts for the NYC TLC's typical data publication delay.

---

## Repository structure

```
taxi_databricks/
├── modules/
│   ├── config.py                   # All catalog, schema, table, URL, and path constants
│   ├── data_loader/
│   │   └── file_downloader.py      # Downloads files from remote URLs to Unity Catalog volumes
│   ├── transformations/
│   │   └── metadata.py             # Adds processed_timestamp column
│   └── utils/
│       └── date_utils.py           # Month boundary helpers
│
├── transformations/
│   └── notebooks/
│       ├── bootstrap.py            # %run this to add project root to sys.path
│       ├── 00_landing/
│       │   ├── ingest_yellow_trips.py   # Download monthly Parquet from NYC TLC
│       │   └── ingest_lookup.py         # Download taxi zone CSV
│       ├── 01_bronze/
│       │   └── yellow_trips_raw.py      # Load Parquet → Bronze Delta table
│       ├── 02_silver/
│       │   ├── yellow_trips_cleansed.py # Decode fields, compute trip duration
│       │   ├── yellow_trips_enriched.py # Join with zone lookup → borough/zone names
│       │   └── taxi_zone_lookup.py      # SCD2 upsert for zone reference data
│       └── 03_gold/
│           └── daily_trip_summary.py    # Daily trip KPI aggregates
│
├── one_off/                        # One-time setup and historical backfill scripts
│   ├── creating_catalogs_schema_volumes.py
│   ├── load_zone_lookup.py
│   └── initial_load/
│       └── notebooks/              # Bulk-load notebooks for historical data
│
├── adhoc/
│   └── Yellow_taxi_Eda.py          # Exploratory analysis notebook
│
└── setup.py                        # Makes modules/ installable as a package
```

---

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- A cluster with access to the `nyctaxi` catalog
- The `delta` Python library (pre-installed on Databricks Runtime)

---

## One-time setup

Run these notebooks **once** in order from the `one_off/` directory:

1. **`creating_catalogs_schema_volumes.py`** — Creates the `nyctaxi` catalog, four schemas (`00_landing`, `01_bronze`, `02_silver`, `03_gold`), and the landing volume.
2. **`load_zone_lookup.py`** — Downloads the taxi zone CSV into the landing volume.
3. **`initial_load/notebooks/`** — Runs the full pipeline over 6 months of historical data (Sept 2025 – Feb 2026) to seed all tables.

### Installing modules as a package (recommended)

To avoid any `sys.path` issues, install the project as a package on your cluster:

```python
# Run in a notebook cell or as a cluster init script
%pip install -e /Workspace/path/to/taxi_databricks
```

Alternatively, every production notebook already calls `%run ../bootstrap` which walks the directory tree to find and register the project root automatically — no manual setup required.

---

## Scheduled pipeline

The production notebooks in `transformations/notebooks/` are designed to run as a **Databricks Job** with dependent tasks in this order:

| Step | Notebook | Description |
|------|----------|-------------|
| 1 | `00_landing/ingest_yellow_trips` | Download monthly Parquet; sets `continue_downstream` task value |
| 2 | `00_landing/ingest_lookup` | Download zone CSV; sets `continue_downstream` task value |
| 3 | `01_bronze/yellow_trips_raw` | Load Parquet to Bronze |
| 4 | `02_silver/yellow_trips_cleansed` | Decode and cleanse trips |
| 5 | `02_silver/taxi_zone_lookup` | SCD2 upsert for zone reference |
| 6 | `02_silver/yellow_trips_enriched` | Enrich trips with borough/zone names |
| 7 | `03_gold/daily_trip_summary` | Aggregate to daily KPIs |

Steps 3–7 only run if step 1 sets `continue_downstream = "yes"`. If the file for the target month was already downloaded in a prior run, the job exits early without duplicating data.

---

## Configuration

All catalog names, schema names, table names, volume paths, and source URLs are defined in a single file — [`modules/config.py`](modules/config.py). To point the pipeline at a different catalog or environment, edit only that file.

| Constant | Default value |
|----------|---------------|
| `CATALOG` | `nyctaxi` |
| `SCHEMA_LANDING` | `00_landing` |
| `SCHEMA_BRONZE` | `01_bronze` |
| `SCHEMA_SILVER` | `02_silver` |
| `SCHEMA_GOLD` | `03_gold` |
| `YELLOW_TAXI_SOURCE_URL` | NYC TLC CloudFront endpoint |
| `ZONE_LOOKUP_URL` | NYC TLC misc endpoint |

---

## Data sources

- **Yellow trip data**: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — monthly Parquet files
- **Taxi zone lookup**: CSV mapping `LocationID` → Borough, Zone, Service Zone
