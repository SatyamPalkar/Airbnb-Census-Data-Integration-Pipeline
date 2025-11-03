# ============================================================
# DAG: Airbnb + Census Bronze Layer ETL  (Assignment 3)
# ============================================================

import logging
from datetime import timedelta
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from google.cloud import storage

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook


# --------------------------
# Configuration
# --------------------------
POSTGRES_CONN_ID = "postgres_default"  # Connection ID defined in Airflow
BUCKET_NAME = "australia-southeast1-bde-at-0ab43f89-bucket"

# All file paths required for the Bronze layer
GCS_FILES = {
    "airbnb": "data/airbnb/05_2020.csv",
    "census_g01": "data/census/2016Census_G01_NSW_LGA.csv",
    "census_g02": "data/census/2016Census_G02_NSW_LGA.csv",
    "mapping_lga": "data/mapping/NSW_LGA_CODE.csv",
    "mapping_suburb": "data/mapping/NSW_LGA_SUBURB.csv",
}

BRONZE_SCHEMA = "bronze"
CHUNKSIZE = 10_000
METHOD = "multi"


# ============================================================
# Default DAG Arguments (Assignment 3 version)
# ============================================================
default_args = {
    "owner": "BDE_Assignment_3",          # updated owner name
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "start_date": days_ago(1),            # Airflow requires past start date
}


# ============================================================
# DAG Definition (Sequential Execution for Stability)
# ============================================================
dag = DAG(
    dag_id="airbnb_bronze_dag",
    default_args=default_args,
    description="Assignment 3: Sequentially load Airbnb and Census CSVs from GCS into Postgres Bronze schema.",
    schedule_interval=None,       # manual trigger
    catchup=False,
    max_active_runs=1,
    concurrency=1,                # ensure only one task runs at a time
    max_active_tasks=1,
    tags=["assignment3", "bronze", "bde", "airbnb"],
)


# ============================================================
# Function to Load CSVs into Postgres
# ============================================================
def load_csv_to_postgres(table_name: str, blob_path: str) -> None:
    logging.info(f"[{table_name}] Starting load from GCS → Postgres")

    # --- Step 1: Download CSV from GCS ---
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_path)
    local_path = f"/tmp/{table_name}.csv"
    blob.download_to_filename(local_path)
    logging.info(f"[{table_name}] Downloaded file from {blob_path}")

    # --- Step 2: Load CSV into pandas ---
    df = pd.read_csv(local_path, dtype=str, keep_default_na=False)
    logging.info(f"[{table_name}] DataFrame loaded with shape {df.shape}")

    # --- Step 3: Connect to Postgres ---
    pg_conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_connection(POSTGRES_CONN_ID)

    if "@" in (pg_conn.host or ""):
        raise ValueError(f"Invalid host format detected in connection: {pg_conn.host}")

    dbname = pg_conn.schema or "postgres"
    user = pg_conn.login or "postgres"
    pwd = quote_plus(pg_conn.password or "")
    host = pg_conn.host
    port = pg_conn.port or 5432

    engine = create_engine(
        f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{dbname}",
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
    )

    # --- Step 4: Create Bronze schema if not exists ---
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA};"))

    # --- Step 5: Load data into Postgres (replace existing) ---
    df.to_sql(
        name=table_name,
        con=engine,
        schema=BRONZE_SCHEMA,
        if_exists="replace",
        index=False,
        chunksize=CHUNKSIZE,
        method=METHOD,
    )

    logging.info(f"[{table_name}] ✅ Successfully loaded into {BRONZE_SCHEMA}.{table_name}")


# ============================================================
# Define Airflow Tasks (PythonOperators)
# ============================================================
load_airbnb = PythonOperator(
    task_id="load_airbnb",
    python_callable=load_csv_to_postgres,
    op_kwargs={"table_name": "airbnb", "blob_path": GCS_FILES["airbnb"]},
    dag=dag,
)

load_census_g01 = PythonOperator(
    task_id="load_census_g01",
    python_callable=load_csv_to_postgres,
    op_kwargs={"table_name": "census_g01", "blob_path": GCS_FILES["census_g01"]},
    dag=dag,
)

load_census_g02 = PythonOperator(
    task_id="load_census_g02",
    python_callable=load_csv_to_postgres,
    op_kwargs={"table_name": "census_g02", "blob_path": GCS_FILES["census_g02"]},
    dag=dag,
)

load_mapping_lga = PythonOperator(
    task_id="load_mapping_lga",
    python_callable=load_csv_to_postgres,
    op_kwargs={"table_name": "mapping_lga", "blob_path": GCS_FILES["mapping_lga"]},
    dag=dag,
)

load_mapping_suburb = PythonOperator(
    task_id="load_mapping_suburb",
    python_callable=load_csv_to_postgres,
    op_kwargs={"table_name": "mapping_suburb", "blob_path": GCS_FILES["mapping_suburb"]},
    dag=dag,
)


# ============================================================
# Sequential Task Dependencies (One-by-One Execution)
# ============================================================
(
    load_airbnb
    >> load_census_g01
    >> load_census_g02
    >> load_mapping_lga
    >> load_mapping_suburb
)
