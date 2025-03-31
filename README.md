

```markdown
# ✈️ EPWA Daily Flights ETL Pipeline

This project is an ETL pipeline that extracts, transforms, and loads data about flights related to the **Warsaw Chopin Airport (EPWA)** using the [Aviationstack API](https://aviationstack.com/). It stores and analyzes both detailed and aggregated flight traffic using **PySpark**, **DuckDB**, and **Apache Airflow**.

---

## 📁 Project Structure

```
project_epwa_daily_traffic/
├── airflow/            # Airflow DAGs and config
├── data/
│   ├── raw/            # Raw JSONs from API
│   └── processed/      
│       ├── daily_traffic/         # Aggregated CSV files (by hour)
│       └── detailed_flights/      # Detailed flattened data
├── db/
│   └── epwa_traffic.duckdb        # DuckDB local database
├── etl/
│   ├── extract.py                 # Downloads latest data from API
│   ├── transform_daily_traffic.py      # Aggregates traffic by hour
│   ├── transform_detailed_scheduled_date.py # Flattens detailed flight data
│   ├── load_epwa_daily_traffic.py         # Loads hourly data into DuckDB
│   └── load_epwa_detailed_flights.py      # Loads detailed data into DuckDB
└── README.md
```

---

## 🛠️ Tools Used

- **Python 3.10**
- **PySpark** – transformation and aggregation
- **DuckDB** – fast local analytical database
- **Apache Airflow** – task orchestration
- **Aviationstack API** – flight data source

---

## 🚀 Features

- ✅ Extracts live departure and arrival data from EPWA
- ✅ Aggregates traffic counts by hour and operation type
- ✅ Extracts full flight metadata (gate, delay, terminal, etc.)
- ✅ Loads data into DuckDB with support for deduplication and updates
- ✅ Airflow DAG handles end-to-end orchestration

---

## 🔐 API Key

To use this project, obtain an API key from [https://aviationstack.com/](https://aviationstack.com/) and insert it into `etl/extract.py`.

---

## 📦 Local Execution

1. Set up Python environment and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. Run ETL manually:
   ```bash
   python etl/extract.py
   python etl/transform_daily_traffic.py
   python etl/load_epwa_daily_traffic.py
   ```

---

## 🧪 Airflow DAG

The Airflow DAG `epwa_flights_pipeline` runs daily and orchestrates the following tasks:

- `extract_from_api`
- `transform_daily_traffic`
- `transform_detailed_flights`
- `load_daily_traffic`
- `load_detailed_flights`

To run Airflow:
```bash
airflow db init
airflow scheduler
airflow webserver
```

---
