from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pathlib import Path

# Directories
base_dir = Path(__file__).resolve().parent.parent.parent
etl_dir = base_dir / "etl"

# Default arguments for the DAG
default_args = {
    "owner": "data-engineer",
    "start_date": datetime(2024, 3, 28),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id="epwa_demo_pipeline",
    default_args=default_args,
    description="Simple demo DAG running extract, transform and load steps",
    schedule_interval=None,
    catchup=False,
    tags=["epwa", "demo"],
) as dag:

    extract_task = BashOperator(
        task_id="extract_from_api",
        bash_command=f"cd {etl_dir} && . ../venv/bin/activate && python extract.py",
    )

    transform_task = BashOperator(
        task_id="transform_daily_traffic",
        bash_command=f"cd {etl_dir} && . ../venv/bin/activate && python transform_daily_traffic.py",
    )

    load_task = BashOperator(
        task_id="load_daily_traffic",
        bash_command=f"cd {etl_dir} && . ../venv/bin/activate && python load_epwa_daily_traffic.py",
    )

    extract_task >> transform_task >> load_task
