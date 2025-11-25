# Lab 2 — End‑to‑End Stock Analytics (Airflow → Snowflake → dbt → Superset)

> **One‑shot README** for cloning and reproducing the project without secrets.  
> Pipelines fetch daily prices with `yfinance`, land them in Snowflake via Airflow, transform with `dbt`, and visualize in Apache Superset.

---

## 📐 High‑Level Architecture

![Data Flow](sandbox:/mnt/data/A_flowchart_diagram_depicts_a_data_processing_pipe.png)

**Flow:** *Airflow* (extract & load) → *Snowflake* (RAW + ANALYTICS schemas) → *dbt* (staging/intermediate/marts) → *Superset* (dashboards).

---

## 📁 Repository Layout

```
.
├── dags/
│   ├── etl_stock_raw_dag.py         # yfinance → Snowflake RAW (idempotent create/use)
│   └── dbt_run_dag.py               # runs `dbt run` against models
├── dbt/
│   ├── dbt_project.yml              # project config (models paths/materializations)
│   ├── profiles.yml                 # points to env vars (no creds stored)
│   └── models/
│       ├── sources.yml              # RAW source (STOCK_PRICES in RAW or PUBLIC)
│       ├── staging/                 # stg_stock_prices (view in ANALYTICS_RAW)
│       ├── intermediate/            # int_with_indicators (view in ANALYTICS_ANALYTICS)
│       └── marts/                   # fct_stock_analytics (table in ANALYTICS_ANALYTICS)
├── requirements.txt                 # pinned deps for reproducible setup
├── .gitignore                       # ignores venvs, logs, dbt target, etc.
└── README.md                        # this file
```

---

## 🔧 Prerequisites

- Python 3.10 recommended (matches Airflow/dbt pins)
- A Snowflake account (role with `USAGE`/`CREATE SCHEMA` on target DB)
- Optional: Homebrew or system tools for virtualenvs
- Superset (local venv) for BI

> **No secrets in the repo** — everything reads from environment variables.

---

## 🧪 Quickstart (Local)

### 1) Clone & create a virtualenv
```bash
git clone <your-repo-url> lab2-stocks
cd lab2-stocks

python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Airflow must be installed with pinned constraints
pip install "apache-airflow==2.9.3" --constraint \
  "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.10.txt"

pip install -r requirements.txt
```

### 2) Export environment variables (no creds in code)
> Replace values with your own. Keep these in your shell or a private `.env` (not committed).

```bash
export SNOWFLAKE_ACCOUNT="SFEDU02-LVB17920"         # Example
export SNOWFLAKE_USER="COYOTE"
export SNOWFLAKE_PASSWORD="<your-password>"
export SNOWFLAKE_ROLE="TRAINING_ROLE"
export SNOWFLAKE_WAREHOUSE="COYOTE_QUERY_WH"
export SNOWFLAKE_DATABASE="USER_DB_COYOTE"

# Where RAW landed (Public or RAW); dbt expects ANALYTICS as target schema by default
export SNOWFLAKE_SCHEMA="ANALYTICS"

# Optional tags/flags
export DBT_PROFILES_DIR="$(pwd)/dbt"
export DBT_PROJECT_DIR="$(pwd)/dbt"
```

> If your **RAW** table lives in `PUBLIC`, the staging model is configured to read from there; otherwise set `raw_schema` Variable in Airflow to `RAW`.

---

## 🌀 Airflow

### Initialize & run services
```bash
export AIRFLOW_HOME="$(pwd)/.airflow"
airflow db init
airflow users create \
  --username admin --firstname Admin --lastname User \
  --role Admin --email admin@example.com --password admin

# In separate terminals
airflow webserver --port 8080
airflow scheduler
```

### Configure connection & variables
- **Connection**: `snowflake_default` (Airflow UI → Admin → Connections)  
  Use the same account/role/warehouse/db as env vars. *Do not* store your password in git.
- **Variables** (Admin → Variables)
  - `snowflake_database` = `USER_DB_COYOTE`
  - `raw_schema` = `RAW` *(or `PUBLIC` based on where you landed)*
  - `analytics_schema` = `ANALYTICS`
  - `snowflake_conn_id` = `snowflake_default`
  - `snowflake_role` = `TRAINING_ROLE`
  - `stock_symbols` = `["AAPL","MSFT","GOOG"]`
  - `start_date` = `2018-01-01`

### Trigger DAGs
- `etl_stock_raw_dag` → loads **STOCK_PRICES** into `RAW` (or `PUBLIC`), creates DB/Schema/table if missing.
- `dbt_run_dag` → executes `dbt run` to build views/tables in ANALYTICS.*

> Check logs in Airflow UI; task logs verify Snowflake SQL and `write_pandas` outcomes.

---

## 🧱 dbt

Project files live in `dbt/`:

- `dbt_project.yml` — models materialization:
  - `staging` → view in `ANALYTICS_RAW`
  - `intermediate` → view in `ANALYTICS_ANALYTICS`
  - `marts` → table in `ANALYTICS_ANALYTICS`
- `profiles.yml` — references only **environment variables**.

Run locally:
```bash
cd dbt
dbt debug
dbt run
```

**Sanity SQL in Snowflake:**
```sql
-- Source table (choose your RAW or PUBLIC)
SELECT COUNT(*) FROM USER_DB_COYOTE.PUBLIC.STOCK_PRICES;

-- Staging view
SELECT COUNT(*) AS "rows" FROM USER_DB_COYOTE.ANALYTICS_RAW.STG_STOCK_PRICES;

-- Final mart
SELECT DATE, SYMBOL, CLOSE, MA20, MA50, RSI14, DAILY_RETURN
FROM USER_DB_COYOTE.ANALYTICS_ANALYTICS.FCT_STOCK_ANALYTICS
ORDER BY DATE DESC, SYMBOL
LIMIT 50;
```

> Column names used: `DATE`, `MA20`, `MA50`, `RSI14`, `DAILY_RETURN`.

---

## 📊 Superset (BI)

Create a separate venv (to avoid Airflow/dbt pin conflicts):
```bash
python3 -m venv superset-venv
source superset-venv/bin/activate
pip install "apache-superset==3.0.2" "snowflake-sqlalchemy==1.7.7" "SQLAlchemy<2.0,>=1.4.49" "numpy==1.23.5" "pandas<2.1,>=1.5.3" "backports.zstd<2"
export FLASK_APP="superset.app:create_app"
superset fab create-admin --username admin --firstname Admin --lastname User --email admin@example.com --password admin
superset db upgrade
superset init
superset run -p 8088
```

Connect **Database** → *Snowflake* using your credentials (not in repo).  
Create **Datasets** from:
- `ANALYTICS_RAW.STG_STOCK_PRICES`
- `ANALYTICS_ANALYTICS.FCT_STOCK_ANALYTICS`

**Example charts you built**
1. **Close Price (Line)** — X: `DATE`, Y: `CLOSE`, Series: `SYMBOL`
2. **RSI (Line)** — X: `DATE`, Y: `RSI14`  
   - Add reference lines at **30** (Oversold) & **70** (Overbought) if available
3. **Daily Return % (Bar)** — X: `DATE`, Y: `DAILY_RETURN` (format as %)
4. **Latest Close (AAPL)** — Table or KPI card filtered on `SYMBOL = 'AAPL'`

Combine charts into a **Dashboard** and save.

---

## ✅ What’s Implemented

- Airflow DAG **etl_stock_raw_dag** (idempotent DDL, yfinance extract, Snowflake load)
- Airflow DAG **dbt_run_dag** (executes `dbt run`)
- Snowflake RAW → ANALYTICS pipeline
- dbt models: staging, indicators (MA20/MA50/RSI14), fact table with daily returns
- Superset visuals: Line (Close), RSI, Daily Return %, Latest Close (AAPL)
- Repro‑safe repo: `requirements.txt`, `.gitignore`, *no secrets committed*

---

## 🧯 Troubleshooting

- **dbt connection failed**: Re‑export env vars; verify `SNOWFLAKE_ROLE` (e.g., `TRAINING_ROLE` vs `TRAINING ROLE`).
- **Airflow SQL invalid identifier `"date"`**: Ensure Snowflake column names align with model expectations and use unquoted identifiers in `write_pandas`. The provided DAG creates `DATE` (unquoted) and lowercases df columns before load.
- **Superset install issues**: keep a **separate** venv and use the pinned versions above. If CLI errors mention Flask app, export `FLASK_APP="superset.app:create_app"`.

---

## 🔐 Security & Secrets

- Credentials are supplied via shell **environment variables** and Airflow **Connections**.  
- `.env` is **ignored** by `.gitignore`. Do not commit secrets.

---

## 🧾 License

Educational use for DATA 200 / DATA 226 lab work. Adapt freely within course guidelines.
