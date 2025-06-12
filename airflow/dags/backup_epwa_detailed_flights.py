from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path

# Paths
base_dir = Path(__file__).resolve().parent.parent.parent
etl_dir = base_dir / "etl"

# Default args
default_args = {
    "owner": "data-engineer",
    "start_date": datetime(2024, 3, 28),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id="epwa_backup_detailed_table",
    default_args=default_args,
    description="Manual DAG to create a backup of the detailed flights table in DuckDB",
    schedule_interval=None,
    catchup=False,
    tags=["epwa", "backup"],
) as dag:

    backup_task = PythonOperator(
        task_id="backup_epwa_detailed_flights",
        python_callable=lambda: __import__("etl.backup_epwa_detailed_flights").backup_epwa_detailed_flights.main(),
    )

