from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd
import io
import os

DAG_ID = "elt_airbnb_census_dbt_full"
POSTGRES_CONN_ID = "postgres_default"
GCS_CONN_ID = "google_cloud_default"
GCS_BUCKET = "australia-southeast1-bde-at-0ab43f89-bucket"
AIRBNB_PATH = "data/airbnb"
CENSUS_PATH = "data/census"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}

def load_csv_to_bronze(prefix_path, table_name, **kwargs):
    """
    Loads CSV files from GCS → Postgres (bronze layer).
    Automatically recreates the table if schema mismatch (including case) is detected.
    """
    gcs_hook = GCSHook(gcp_conn_id=GCS_CONN_ID)
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    files = gcs_hook.list(bucket_name=GCS_BUCKET, prefix=prefix_path)
    csv_files = sorted([f for f in files if f.endswith(".csv")])
    print(f"📂 Found {len(csv_files)} CSV files in {prefix_path}: {csv_files}")

    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    conn.commit()

    for file_path in csv_files:
        print(f"\n Processing: {file_path}")
        file_bytes = gcs_hook.download(bucket_name=GCS_BUCKET, object_name=file_path)
        df = pd.read_csv(io.BytesIO(file_bytes))
        df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
        df.columns = [c.strip().replace(" ", "_") for c in df.columns]  


        cur.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'bronze' AND table_name = '{table_name.lower()}';
        """)
        existing_cols = [r[0] for r in cur.fetchall()]
        new_cols = list(df.columns)


        existing_lower = set(map(str.lower, existing_cols))
        new_lower = set(map(str.lower, new_cols))

        if not existing_cols or existing_lower != new_lower:
            print(f"Detected schema or case mismatch for bronze.{table_name} — rebuilding table...")
            cur.execute(f"DROP TABLE IF EXISTS bronze.{table_name} CASCADE;")
            col_defs = ", ".join([f'"{c.lower()}" TEXT' for c in df.columns])
            cur.execute(f"CREATE TABLE bronze.{table_name} ({col_defs});")
            conn.commit()
        else:
            print(f"Schema matches existing bronze.{table_name}")


        df.columns = [c.lower() for c in df.columns]


        quoted_cols = ', '.join([f'"{c}"' for c in df.columns])
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_sql = f"INSERT INTO bronze.{table_name} ({quoted_cols}) VALUES ({placeholders});"
        cur.executemany(insert_sql, df.values.tolist())
        conn.commit()

        print(f"Loaded {len(df)} rows into bronze.{table_name}")

    cur.close()
    conn.close()
    print(f"All {table_name} files loaded successfully into bronze schema.")

def load_airbnb_to_bronze(**kwargs):
    load_csv_to_bronze(prefix_path=AIRBNB_PATH, table_name="airbnb_raw", **kwargs)

def load_census_to_bronze(**kwargs):
    load_csv_to_bronze(prefix_path=CENSUS_PATH, table_name="census_raw", **kwargs)

def validate_bronze_data():
    """Simple row-count validation"""
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = postgres_hook.get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM bronze.airbnb_raw;")
    airbnb_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM bronze.census_raw;")
    census_count = cur.fetchone()[0]

    print(f"Airbnb bronze rows: {airbnb_count}")
    print(f"Census bronze rows: {census_count}")

    cur.close()
    conn.close()

def run_dbt_pipeline():
    dbt_project_dir = "/home/airflow/gcs/dags/dbt/airbnb_dbt_project"
    print("Running dbt transformations...")
    os.system(f"cd {dbt_project_dir} && dbt deps && dbt build --target prod")
    print("dbt transformations complete!")

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["elt", "airbnb", "census", "dbt", "bronze", "postgres"],
) as dag:

    load_airbnb = PythonOperator(
        task_id="load_airbnb_to_bronze",
        python_callable=load_airbnb_to_bronze,
    )

    load_census = PythonOperator(
        task_id="load_census_to_bronze",
        python_callable=load_census_to_bronze,
    )

    validate_bronze = PythonOperator(
        task_id="validate_bronze_data",
        python_callable=validate_bronze_data,
    )

    run_dbt = PythonOperator(
        task_id="run_dbt_pipeline",
        python_callable=run_dbt_pipeline,
    )

    [load_airbnb, load_census] >> validate_bronze >> run_dbt
