from airflow import DAG
from airflow.operators.bash import BashOperator
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

    transform_task = BashOperator(
        task_id="transform_detailed_flights",
        bash_command=f"cd {etl_dir} && . ../venv/bin/activate && python transform_detailed_scheduled_date.py",
    )

    load_task = BashOperator(
        task_id="load_detailed_flights",
        bash_command=f"cd {etl_dir} && . ../venv/bin/activate && python load_epwa_detailed_flights.py",
    )

    transform_task >> load_task
