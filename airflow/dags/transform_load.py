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
    dag_id="epwa_transform_and_load_detailed",
    default_args=default_args,
    description="Transform and Load detailed flight data from existing JSON files",
    schedule_interval=None,  # Run manually only
    catchup=False,
    tags=["epwa", "manual", "transform", "load"],
) as dag:

    transform_task = PythonOperator(
        task_id="transform_detailed_flights",
        python_callable=lambda: __import__("etl.transform_detailed_scheduled_date").transform_detailed_scheduled_date.main(),
    )

    load_task = PythonOperator(
        task_id="load_detailed_flights",
        python_callable=lambda: __import__("etl.load_epwa_detailed_flights").load_epwa_detailed_flights.main(),
    )

    transform_task >> load_task

