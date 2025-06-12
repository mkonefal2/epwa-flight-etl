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
    dag_id="epwa_enrich_arrival_country",
    default_args=default_args,
    description="Add arrival_country field to flight details",
    schedule_interval=None,
    catchup=False,
    tags=["epwa", "enrichment"],
) as dag:

    enrich_arrival_country = BashOperator(
        task_id="enrich_arrival_country",
        bash_command=f"cd {etl_dir} && source ../venv/bin/activate && python enrich_epwa_country_detailed_flights.py",
    )
