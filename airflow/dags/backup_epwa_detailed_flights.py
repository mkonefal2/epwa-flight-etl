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
    dag_id="epwa_backup_detailed_table",
    default_args=default_args,
    description="Manual DAG to create a backup of the detailed flights table in DuckDB",
    schedule_interval=None,
    catchup=False,
    tags=["epwa", "backup"],
) as dag:

    backup_task = BashOperator(
        task_id="backup_epwa_detailed_flights",
        bash_command=f"cd {etl_dir} && . ../venv/bin/activate && python backup_epwa_detailed_flights.py",
    )
