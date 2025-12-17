# **Santa Clara Crash Analytics Pipeline — DATA226 - Group Project **
*(Airflow → Snowflake → dbt → Tableau)*

## 📘 Overview
This project implements a production-oriented ELT (Extract–Load–Transform) pipeline for analyzing Santa Clara County traffic accident data.

Pipeline steps:
1. Extraction — crash CSV + real-time weather + traffic API data  
2. Loading — raw data stored in Snowflake RAW schema  
3. Transformation — dbt models (staging → intermediate → marts)  
4. Visualization — Tableau dashboards for hotspots, trends, and risk analysis  

---

## 🧱 Architecture Diagram

To include the Mermaid diagram, paste the following into GitHub:

\`\`\`mermaid
flowchart LR
    CSV[Historical Crash Data\n(CSV)] --> A[Airflow Ingestion DAGs]
    WEATHER[OpenWeather API] --> A
    TRAFFIC[Google Distance Matrix API] --> A
    A --> RAW[Snowflake RAW Schema]
    RAW --> DBT[dbt Models: Staging → Intermediate → Marts]
    DBT --> MART[Snowflake MART Schema]
    MART --> TABLEAU[Tableau Dashboards]
    TABLEAU --> INSIGHTS[Risk Hotspots\nWeather Impact\nCrash Forecasts]
\`\`\`

---

## 📁 Repository Structure

\`\`\`
.
├── dags/                         # Airflow DAGs for ingestion + dbt
├── data/                         # Historical accident dataset(s)
├── tableau/                      # Tableau dashboards / screenshots
├── compose.yaml                  # Docker Compose for Airflow
└── README.md
\`\`\`

---

## 🔧 Prerequisites
- Python 3.10+
- Docker + Docker Compose
- Snowflake account
- dbt-core + dbt-snowflake
- Tableau Desktop / Public
- API keys: OpenWeatherMap + Google Distance Matrix

---

## 🔐 Environment Variables

\`\`\`
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
\`\\"\`

---

## 🌀 Airflow Setup

### Start Airflow
\`\`\`
docker-compose -f compose.yaml up --build
\`\`\`

### Airflow Connection (snowflake_conn)
Account  
User  
Password  
Warehouse: COMPUTE_WH  
Database: ACCIDENT_DW  
Schema: RAW  
Role: DATA226_ROLE  

### Airflow Variables
snowflake_database = ACCIDENT_DW  
raw_schema = RAW  
intermediate_schema = INT  
mart_schema = MART  
openweather_api_key = <key>  
traffic_api_key = <key>  

---

## 📡 DAGs

### ingest_crash_data
- Load CSV → RAW schema  
- Validate row count  

### ingest_weather_data
- Pull OpenWeatherMap data  
- Store in RAW schema  

### ingest_traffic_data
- Pull Google Distance Matrix travel-time + congestion  

### run_dbt_pipeline
- dbt run + test  
- Builds:
  - staging  
  - intermediate  
  - marts  

---

## 🧱 dbt Layer

Run:
\`\`\`
dbt debug
dbt run
dbt test
\`\`\`

Snowflake checks:
\`\`\`
SELECT COUNT(*) FROM RAW.CRASHES;
SELECT * FROM MART.FACT_CRASHES LIMIT 20;
\`\`\`

---

## 📊 Tableau Dashboard

Snowflake Connection:
- Warehouse: COMPUTE_WH  
- Database: ACCIDENT_DW  
- Schema: MART  

Dashboard visuals:
- Monthly crash trends  
- Severity category  
- Weather × traffic control heatmap  
- Road condition charts  
- Geospatial hotspots  
- Crash forecast trends  

---

## 📄 License  
For academic use in DATA 226 — San José State University.
