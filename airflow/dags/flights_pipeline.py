from airflow import DAG
from airflow.operators.python import PythonOperator
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

    extract_task = PythonOperator(
        task_id="extract_from_api",
        python_callable=lambda: __import__("etl.extract").extract.main(),
    )

    transform_daily_traffic = PythonOperator(
        task_id="transform_daily_traffic",
        python_callable=lambda: __import__("etl.transform_daily_traffic").transform_daily_traffic.main(),
    )

    transform_detailed_flights = PythonOperator(
        task_id="transform_detailed_flights",
        python_callable=lambda: __import__("etl.transform_detailed_scheduled_date").transform_detailed_scheduled_date.main(),
    )

    load_daily_traffic = PythonOperator(
        task_id="load_daily_traffic",
        python_callable=lambda: __import__("etl.load_epwa_daily_traffic").load_epwa_daily_traffic.main(),
    )

    load_detailed_flights = PythonOperator(
        task_id="load_detailed_flights",
        python_callable=lambda: __import__("etl.load_epwa_detailed_flights").load_epwa_detailed_flights.main(),
    )

    enrich_arrival_country = PythonOperator(
        task_id="enrich_arrival_country",
        python_callable=lambda: __import__("etl.enrich_epwa_country_detailed_flights").enrich_epwa_country_detailed_flights.main(),
    )

    extract_task >> [transform_daily_traffic, transform_detailed_flights]
    transform_daily_traffic >> load_daily_traffic
    transform_detailed_flights >> load_detailed_flights >> enrich_arrival_country

