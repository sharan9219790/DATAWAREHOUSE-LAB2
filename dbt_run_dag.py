# dbt_run_dag.py
#lab 2
from __future__ import annotations

import os
import shlex
import logging
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook

# -----------------------
# Project locations (YOUR paths)
# -----------------------
DBT_PROJECT_DIR = "/Users/spartan/Downloads/DATA226/LAB2new/dbt"
DBT_PROFILES_DIR = "/Users/spartan/Downloads/DATA226/LAB2new/dbt"

# -----------------------
# Airflow config knobs
# -----------------------
SNOWFLAKE_CONN_ID = Variable.get("snowflake_conn_id", default_var="snowflake_default")

# Optional: set a query tag so runs are easy to spot in Snowflake history
DBT_QUERY_TAG = Variable.get("dbt_query_tag", default_var="dbt-lab2")

# Fallbacks if the connection extras don’t include these
DB_NAME_DEFAULT      = Variable.get("snowflake_database", default_var="USER_DB_COYOTE")
WAREHOUSE_DEFAULT    = Variable.get("snowflake_warehouse", default_var="COYOTE_QUERY_WH")
ROLE_DEFAULT         = Variable.get("snowflake_role", default_var="TRAINING_ROLE")  # underscore!
SCHEMA_DEFAULT       = Variable.get("snowflake_schema", default_var="ANALYTICS")

def _get_conn_field(conn, *keys, default=None):
    """
    Try several keys in conn.extra_dejson; if not found, try conn.<attr>; else default.
    Handy because Snowflake connections can store fields in different places.
    """
    # first look into extras
    extra = conn.extra_dejson or {}
    for k in keys:
        if k in extra and extra[k]:
            return extra[k]
        # airflow sometimes nests as 'extra__snowflake__account'
        namespaced = f"extra__snowflake__{k}"
        if namespaced in extra and extra[namespaced]:
            return extra[namespaced]

    # then look at common base fields
    for k in keys:
        if hasattr(conn, k):
            val = getattr(conn, k)
            if val:
                return val

    return default

def _build_dbt_env_from_airflow_conn() -> dict:
    """
    Build env vars for dbt from your existing Airflow Snowflake connection (no hardcoding).
    """
    conn = BaseHook.get_connection(SNOWFLAKE_CONN_ID)

    # login/password come from standard connection fields
    user = conn.login
    password = conn.password

    # the rest we try to pull from extras, with safe fallbacks
    account   = _get_conn_field(conn, "account", "host", default=None)
    warehouse = _get_conn_field(conn, "warehouse", default=WAREHOUSE_DEFAULT)
    role      = _get_conn_field(conn, "role", default=ROLE_DEFAULT)
    database  = _get_conn_field(conn, "database", default=DB_NAME_DEFAULT)
    schema    = _get_conn_field(conn, "schema", default=SCHEMA_DEFAULT)

    if not account:
        # If host was set like "xyz.snowflakecomputing.com", trim the domain to get account
        host = _get_conn_field(conn, "host", default="")
        if host and ".snowflakecomputing.com" in host:
            account = host.split(".snowflakecomputing.com")[0]
    if not account:
        raise ValueError(
            "Could not determine Snowflake 'account' from the connection. "
            "Populate it in the connection extras as 'account' (e.g. SFEDU02-LVB17920)."
        )

    env = {
        # dbt looks for these env vars in your profiles.yml jinja
        "SNOWFLAKE_ACCOUNT": account,
        "SNOWFLAKE_USER": user,
        "SNOWFLAKE_PASSWORD": password,
        "SNOWFLAKE_WAREHOUSE": warehouse,
        "SNOWFLAKE_ROLE": role,                 # must be TRAINING_ROLE (underscore)
        "SNOWFLAKE_DATABASE": database,
        "SNOWFLAKE_SCHEMA": schema,
        "DBT_QUERY_TAG": DBT_QUERY_TAG,
    }

    # make sure PATH includes the venv bin so 'dbt' resolves
    venv_bin = os.path.dirname(os.sys.executable)
    env["PATH"] = f"{venv_bin}:{os.environ.get('PATH','')}"

    return env

# -----------------------
# DAG definition
# -----------------------
default_args = {
    "owner": "data226",
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dbt_run_dag",
    description="Run dbt (deps, run, test) against Snowflake using Airflow connection creds",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,       # manual for now; set '@daily' later if desired
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "snowflake", "analytics"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Print versions and connection info
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=(
            f"cd {shlex.quote(DBT_PROJECT_DIR)} && "
            f"dbt --version && "
            f"dbt debug --profiles-dir {shlex.quote(DBT_PROFILES_DIR)} --project-dir {shlex.quote(DBT_PROJECT_DIR)} -t dev"
        ),
        env=_build_dbt_env_from_airflow_conn(),
    )

    # Install packages (just in case you add any later)
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"cd {shlex.quote(DBT_PROJECT_DIR)} && "
            f"dbt deps --profiles-dir {shlex.quote(DBT_PROFILES_DIR)} --project-dir {shlex.quote(DBT_PROJECT_DIR)}"
        ),
        env=_build_dbt_env_from_airflow_conn(),
    )

    # Run all models in your project
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {shlex.quote(DBT_PROJECT_DIR)} && "
            f"dbt run --profiles-dir {shlex.quote(DBT_PROFILES_DIR)} --project-dir {shlex.quote(DBT_PROJECT_DIR)} -t dev"
        ),
        env=_build_dbt_env_from_airflow_conn(),
    )

    # Optional: run tests you defined in schema.yml
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {shlex.quote(DBT_PROJECT_DIR)} && "
            f"dbt test --profiles-dir {shlex.quote(DBT_PROFILES_DIR)} --project-dir {shlex.quote(DBT_PROJECT_DIR)} -t dev"
        ),
        env=_build_dbt_env_from_airflow_conn(),
    )

    end = EmptyOperator(task_id="end")

    start >> dbt_debug >> dbt_deps >> dbt_run >> dbt_test >> end

