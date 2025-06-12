from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pathlib import Path

# 📌 Calculating paths
dag_dir = Path(__file__).resolve().parent
project_root = dag_dir.parent.parent
etl_dir = project_root / "etl"

# ⚙️ DAG parameters
default_args = {
    "owner": "data-engineer",
    "start_date": datetime(2024, 3, 28),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# 🚀 DAG
with DAG(
    dag_id="epwa_flights_pipeline",
    default_args=default_args,
    description="EPWA Daily and Detailed Flights ETL",
    schedule_interval="@daily",
    catchup=False,
) as dag:

    extract_task = BashOperator(
        task_id="extract_from_api",
        bash_command=f"cd {etl_dir} && source ../venv/bin/activate && python extract.py",
    )

    transform_daily_traffic = BashOperator(
        task_id="transform_daily_traffic",
        bash_command=f"cd {etl_dir} && source ../venv/bin/activate && python transform_daily_traffic.py",
    )

    transform_detailed_flights = BashOperator(
        task_id="transform_detailed_flights",
        bash_command=f"cd {etl_dir} && source ../venv/bin/activate && python transform_detailed_scheduled_date.py",
    )

    load_daily_traffic = BashOperator(
        task_id="load_daily_traffic",
        bash_command=f"cd {etl_dir} && source ../venv/bin/activate && python load_epwa_daily_traffic.py",
    )

    load_detailed_flights = BashOperator(
        task_id="load_detailed_flights",
        bash_command=f"cd {etl_dir} && source ../venv/bin/activate && python load_epwa_detailed_flights.py",
    )

    enrich_arrival_country = BashOperator(
        task_id="enrich_arrival_country",
        bash_command=f"cd {etl_dir} && source ../venv/bin/activate && python enrich_epwa_country_detailed_flights.py",
    )

    extract_task >> [transform_daily_traffic, transform_detailed_flights]
    transform_daily_traffic >> load_daily_traffic
    transform_detailed_flights >> load_detailed_flights >> enrich_arrival_country