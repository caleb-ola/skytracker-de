# ✈️ SkyTrack DE

SkyTrack DE is an end-to-end data engineering pipeline that ingests, processes, and models near real-time flight data using PySpark. It fetches live flight states via the OpenSky API and enriches them with airline and aircraft metadata from OpenFlights.

---

## 🧱 Project Structure

```bash
SkyTrack DE/
│
├── data/
│ ├── bronze/ # Raw ingested JSON files
│ ├── silver/ # Cleaned intermediate files
│ └── gold/ # Final fact/dimension tables (Parquet)
│
├── scripts/
│ ├── ingest.py # Ingests flight data from OpenSky API
│ └── etl.py # Transforms data into star schema tables
│
├── utils/
│ └── udfs.py # Custom PySpark UDFs
│
├── logs/ # Stores log files
│
├── orchestrator.sh # Bash script to run full pipeline
├── requirements.txt
└── README.md

---
```

## 🚀 Features

- Ingests near real-time flight data from the OpenSky API
- Enriches flight data with airport, airline and aircraft metadata from OpenFlights
- Cleans and transforms data using PySpark
- Builds a star schema (fact + dimension tables)
- Stores data in Parquet files locally (Gold layer)
- Logs all stages and tracks run history
- Designed for easy migration to cloud (e.g., AWS S3, Redshift)

---

## ⚙️ Tech Stack

- **Language:** Python 3.12
- **Framework:** PySpark
- **Orchestration:** Bash + Cron
- **Data Source:** OpenSky API, OpenFlights CSV
- **Storage:** Local Parquet, optional PostgreSQL
- **Logging:** Python `logging` module

---

## 📦 Setup Instructions

```bash
# Step 1: Clone the repo
git clone https://github.com/your-username/skytrack-de.git
cd skytrack-de

# Step 2: Create Python environment
conda create -n skytrack-env python=3.12
conda activate skytrack-env

# Step 3: Install dependencies
pip install -r requirements.txt
```

## 🧪 Running the Pipeline

#### Option 1: Manual Run

```bash
bash orchestrator.sh
```

#### Option 2: Schedule with Cron (Every 15 Minutes)

```bash
*/15 * * * * /path/to/SkyTrack\ DE/orchestrator.sh >> /path/to/SkyTrack\ DE/logs/cron.log 2>&1
```

## 🛠️ Data Model (Star Schema)

#### 📊 Fact Table: fact_flights

```bash
Column Description
flight_id Unique snapshot ID
aircraft_id FK to aircraft
airline_id FK to airline
timestamp UTC snapshot time
callsign Aircraft callsign
geo_distance_km Haversine distance (lat/lon points)
```

#### 📁 Dimension Table: dim_airlines

```bash
Column Description
airline_id Primary Key
airline_name Name of the airline
icao ICAO code
callsign Radio callsign
country Country
active Y/N flag
```

#### 🛩️ Dimension Table: dim_aircrafts

Column Description
aircraft_id Primary Key
manufacturer Aircraft maker
type_code Model code
registration Registration number
icao24 ICAO hex identifier

## 📈 Monitoring & Logging

Logs written to logs/etl.log

#### To view logs:

```bash

tail -f logs/etl.log
```

## 📊 Possible Future Improvements

Migrate to Apache Airflow or Prefect for orchestration

Add Dockerfile for containerization

Store output in cloud storage (S3, Redshift, BigQuery)

Add unit tests (pytest)

Build dashboard (e.g., with Streamlit or Metabase)

## 📚 Data Sources

OpenSky Network API
https://opensky-network.org/apidoc/
Provides real-time aircraft state data (JSON).

OpenFlights Airline/Aircraft CSVs
https://openflights.org/data.html
Provides metadata for enrichment.

## 👤 Author

Caleb Amao
GitHub: @caleb-ola
LinkedIn:
