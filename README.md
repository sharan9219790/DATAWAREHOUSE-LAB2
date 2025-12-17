# Accident Analytics Pipeline — DATA226
(Airflow → Snowflake → dbt → Tableau)

## 📘 Overview
This project implements an ELT pipeline for analyzing traffic accident data from Santa Clara County.

Pipeline steps:
1. Extraction — crash CSV, live weather API, live traffic API  
2. Loading — raw data stored in Snowflake RAW schema  
3. Transformation — dbt models (staging → intermediate → marts)  
4. Visualization — Tableau dashboards (hotspots, trends, weather risk, forecasting)

---

## 🧱 Architecture Diagram (Mermaid)

Paste this directly into GitHub (outside this code block):

    mermaid
    flowchart LR
        CSV[Historical Crash Data] --> A[Airflow Ingestion DAGs]
        WEATHER[OpenWeather API] --> A
        TRAFFIC[Google Distance Matrix API] --> A
        A --> RAW[Snowflake RAW Schema]
        RAW --> DBT[dbt Models]
        DBT --> MART[Snowflake MART Schema]
        MART --> TABLEAU[Tableau Dashboards]
        TABLEAU --> INSIGHTS[Risk Hotspots, Weather Impact, Crash Forecasts]

---

## 📁 Repository Structure

    .
    ├── dags/                         # Airflow DAGs (ingestion + dbt)
    ├── data/                         # Historical accident dataset(s)
    ├── tableau/                      # Tableau dashboards / screenshots
    ├── compose.yaml                  # Docker Compose for Airflow
    └── README.md

---

## 🔧 Prerequisites
- Python 3.10+
- Docker + Docker Compose
- Snowflake account
- dbt-core + dbt-snowflake
- Tableau Desktop / Public
- API keys: OpenWeatherMap + Google Distance Matrix

---

## 🔐 Required Environment Variables

    export SNOWFLAKE_ACCOUNT="<account>"
    export SNOWFLAKE_USER="<user>"
    export SNOWFLAKE_PASSWORD="<password>"
    export SNOWFLAKE_ROLE="DATA226_ROLE"
    export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
    export SNOWFLAKE_DATABASE="ACCIDENT_DW"
    export SNOWFLAKE_SCHEMA="RAW"

    export OPENWEATHER_API_KEY="<weather_key>"
    export GOOGLE_DISTANCE_MATRIX_API_KEY="<maps_key>"

    export DBT_PROFILES_DIR="$(pwd)/dbt"
    export AIRFLOW_HOME="$(pwd)/.airflow"

---

## 🌀 Airflow Configuration

### 1. Start Airflow

    docker-compose -f compose.yaml up --build

### 2. Airflow UI

    http://localhost:8080
    username: airflow
    password: airflow

### 3. Snowflake Connection (snowflake_conn)

    Conn Type: Snowflake
    Account: <account>
    User: <user>
    Password: <password>
    Warehouse: COMPUTE_WH
    Database: ACCIDENT_DW
    Schema: RAW
    Role: DATA226_ROLE

### 4. Airflow Variables

    snowflake_database = ACCIDENT_DW
    raw_schema = RAW
    intermediate_schema = INT
    mart_schema = MART
    openweather_api_key = <key>
    traffic_api_key = <key>

---

## 📡 DAGs

### ingest_crash_data
- Loads crash CSV into RAW  
- Creates tables if needed  
- Validates row counts  

### ingest_weather_data
- Calls OpenWeather API  
- Stores weather snapshots  

### ingest_traffic_data
- Calls Google Distance Matrix API  
- Stores congestion + travel-time data  

### run_dbt_pipeline
- Runs dbt models:
    - staging  
    - intermediate  
    - marts: FACT_CRASHES, DIM_WEATHER, DIM_LOCATION, DIM_TRAFFIC, DIM_DATE  

---

## 🧱 dbt Layer

Run commands:

    dbt debug
    dbt run
    dbt test

Validation queries:

    SELECT COUNT(*) FROM RAW.CRASHES;
    SELECT * FROM MART.FACT_CRASHES LIMIT 20;

---

## 📊 Tableau Dashboard

Snowflake connection:

    Warehouse: COMPUTE_WH
    Database: ACCIDENT_DW
    Schema: MART

Recommended visuals:
- Crash trends by month  
- Severity distribution  
- Weather × traffic control heatmaps  
- Road surface & lighting analysis  
- Geo accident hotspots  
- Crash forecasting  

---

## 📄 License
Educational use — DATA 226 (San José State University)
