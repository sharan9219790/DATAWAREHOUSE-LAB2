# etl_stock_raw_dag.py
from __future__ import annotations

import json
from datetime import timedelta
import pandas as pd
import yfinance as yf
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

# -----------------------
# Defaults (can be overridden by Airflow Variables)
# -----------------------
DB_NAME = Variable.get("snowflake_database", default_var="USER_DB_COYOTE")
RAW_SCHEMA = Variable.get("raw_schema", default_var="RAW")
ANALYTICS_SCHEMA = Variable.get("analytics_schema", default_var="ANALYTICS")

SNOWFLAKE_CONN_ID = Variable.get("snowflake_conn_id", default_var="snowflake_default")
SNOWFLAKE_ROLE = Variable.get("snowflake_role", default_var="TRAINING ROLE").strip()

STOCK_SYMBOLS = Variable.get(
    "stock_symbols", default_var='["AAPL","MSFT","GOOG"]', deserialize_json=True
)
START_DATE = Variable.get("start_date", default_var="2018-01-01")

RAW_TABLE = "STOCK_PRICES"

# -----------------------
# Helpers
# -----------------------
def _quote_ident_if_needed(name: str) -> str:
    if not name:
        return name
    if any(ch.isspace() for ch in name) or '"' in name:
        return '"' + name.replace('"', '""') + '"'
    return name

def _maybe_role_stmt() -> list[str]:
    if SNOWFLAKE_ROLE:
        return [f"USE ROLE {_quote_ident_if_needed(SNOWFLAKE_ROLE)}"]
    return []

def _run_sql_list(cur, statements: list[str]):
    for sql in statements:
        logging.info("Running SQL: %s", sql)
        cur.execute(sql)

def _ensure_db_schema_and_table(cur):
    stmts = []
    stmts += _maybe_role_stmt()
    stmts += [
        f"CREATE DATABASE IF NOT EXISTS {DB_NAME}",
        f"USE DATABASE {DB_NAME}",
        f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}",
        f"""
        CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{RAW_TABLE} (
            DATE         DATE,
            OPEN         FLOAT,
            HIGH         FLOAT,
            LOW          FLOAT,
            CLOSE        FLOAT,
            ADJ_CLOSE    FLOAT,
            VOLUME       NUMBER,
            SYMBOL       STRING
        )
        """,
        f"CREATE SCHEMA IF NOT EXISTS {ANALYTICS_SCHEMA}",
    ]
    _run_sql_list(cur, stmts)

def _fetch_prices_df() -> pd.DataFrame:
    symbols = STOCK_SYMBOLS
    if not isinstance(symbols, (list, tuple)):
        symbols = [symbols]

    # Keep Adj Close by disabling auto-adjust
    df = yf.download(symbols, start=START_DATE, progress=False, auto_adjust=False)

    if df.empty:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "adj_close", "volume", "symbol"])

    if isinstance(df.columns, pd.MultiIndex):
        # Normalize: rows = (date, symbol), columns = fields
        df = (
            df.swaplevel(0, 1, axis=1)       # top level = ticker
              .sort_index(axis=1)
              .stack(level=0)                # stack tickers into the index
        )
        # After stack, index levels are usually ['Date', 'level_0'] (ticker)
        df.index.names = ['date', 'symbol']
        df = df.reset_index()
    else:
        # Single ticker case
        df = df.reset_index()
        df["symbol"] = symbols[0] if symbols else None

    # tidy names
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    # uniform column set
    rename_map = {
        "adj_close": "adj_close",
        "close": "close",
        "open": "open",
        "high": "high",
        "low": "low",
        "volume": "volume",
        "date": "date",
        "symbol": "symbol",
        "adjclose": "adj_close",  # sometimes appears without underscore
    }
    df = df.rename(columns=rename_map)

    # if adj_close missing, fall back to close
    if "adj_close" not in df.columns and "close" in df.columns:
        df["adj_close"] = df["close"]

    expected = ["date", "open", "high", "low", "close", "adj_close", "volume", "symbol"]
    df = df[[c for c in expected if c in df.columns]]

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")
    if "symbol" in df.columns:
        # keep actual NULLs for missing symbols (don’t coerce to 'nan')
        pass

    return df


# -----------------------
# Task callables
# -----------------------
def debug_connection(**_):
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        stmts = _maybe_role_stmt() + [
            "SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()"
        ]
        _run_sql_list(cur, stmts)
        row = cur.fetchone()
        logging.info("Session -> ROLE=%s | WH=%s | DB=%s | SCHEMA=%s", row[0], row[1], row[2], row[3])

def init_schemas_and_table(**_):
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        _ensure_db_schema_and_table(cur)

def extract_prices(**_):
    df = _fetch_prices_df()
    logging.info("Fetched %d rows for symbols: %s", len(df), STOCK_SYMBOLS)
    return df.to_json(orient="records", date_format="iso")

def load_to_snowflake(**context):
    import json
    ti = context["ti"]

    # Pull JSON that extract_prices returned
    json_records = ti.xcom_pull(task_ids="extract_prices")
    if not json_records:
        logging.warning("No data from XCom.")
        return

    records = json.loads(json_records)
    if not records:
        logging.warning("Empty records.")
        return

    # Build DataFrame and make columns match Snowflake table (UPPERCASE)
    df = pd.DataFrame.from_records(records)

    # keep only expected columns (if present) and order them
    expected_lower = ["date", "open", "high", "low", "close", "adj_close", "volume", "symbol"]
    df = df[[c for c in expected_lower if c in df.columns]]

    # dtypes
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str)

    # >>> IMPORTANT: make column names UPPERCASE to match table DDL <<<
    df.columns = [c.upper() for c in df.columns]

    # Connect & load
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        # Ensure DB/Schema/Table exist (and USE DATABASE)
        _ensure_db_schema_and_table(cur)

        # Optional: if you keep a warehouse in Variables, use it (no-op if not set)
        wh = Variable.get("snowflake_warehouse", default_var=None)
        if wh:
            logging.info('Running SQL: USE WAREHOUSE "%s"', wh)
            cur.execute(f'USE WAREHOUSE "{wh}"')

        # Write to RAW schema
        logging.info("Starting write_pandas into %s.%s.%s rows=%d",
                     DB_NAME, RAW_SCHEMA, RAW_TABLE, len(df))

        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=RAW_TABLE,
            database=DB_NAME,
            schema=RAW_SCHEMA,
            quote_identifiers=True,   # OK because columns are UPPERCASE now
            overwrite=False           # append
        )
        logging.info("write_pandas -> success=%s chunks=%s rows=%s", success, nchunks, nrows)
        if not success or nrows == 0:
            raise RuntimeError(f"write_pandas reported success={success}, rows={nrows}")

# -----------------------
# DAG
# -----------------------
default_args = {
    "owner": "data226",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="etl_stock_raw_dag",
    description="Fetch stock prices via yfinance and load to Snowflake RAW schema",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "snowflake", "raw"],
) as dag:

    start = EmptyOperator(task_id="start")

    t_debug = PythonOperator(
        task_id="debug_connection",
        python_callable=debug_connection,
    )

    t_init = PythonOperator(
        task_id="init_schemas_and_table",
        python_callable=init_schemas_and_table,
    )

    t_extract = PythonOperator(
        task_id="extract_prices",
        python_callable=extract_prices,
    )

    t_load = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
    )

    end = EmptyOperator(task_id="end")

    start >> t_debug >> t_init >> t_extract >> t_load >> end
