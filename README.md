# ✈️ EPWA Daily Flights ETL Pipeline

This project is an ETL pipeline that extracts, transforms, and loads data about flights related to the **Warsaw Chopin Airport (EPWA)** using the [Aviationstack API](https://aviationstack.com/). It stores and analyzes both detailed and aggregated flight traffic using **PySpark**, **DuckDB**, and **Apache Airflow**.

---

## 📦 Quick Start (Automated Setup)

You can set up the entire environment automatically on a fresh Ubuntu machine.

### 🔧 Installation Steps

1. Clone this repository (the installer script is included):
   ```bash
   git clone https://github.com/mkonefal2/epwa-flight-etl.git
   cd epwa-flight-etl
   ```

2. Run the installer (requires sudo):
   ```bash
   chmod +x install_and_start.sh
   sudo ./install_and_start.sh
   ```

3. After the script finishes, open:
   ```
   http://<your-vm-ip>:8080
   ```

   Default Airflow credentials:
   - **Username:** `admin`
   - **Password:** `StrongPassword123`

---

## 📁 Project Structure

```
epwa-flight-etl/
├── airflow/                      # Airflow DAGs and configuration
├── data/
│   ├── raw/                      # Raw JSONs from Aviationstack API
│   └── processed/
│       ├── daily_traffic/       # Aggregated CSVs by hour
│       └── detailed_flights/    # Flattened detailed data
├── db/
│   └── epwa_traffic.duckdb      # DuckDB database
├── etl/
│   ├── extract.py
│   ├── transform_daily_traffic.py
│   ├── transform_detailed_scheduled_date.py
│   ├── load_epwa_daily_traffic.py
│   └── load_epwa_detailed_flights.py
└── requirements.txt
```

---

## 🛠️ Tools Used

- **Python 3.10**
- **PySpark** – data processing and aggregation
- **DuckDB** – analytical database
- **Apache Airflow** – orchestration of DAGs
- **Aviationstack API** – source of flight data

---

## 🔐 API Key

Register at [https://aviationstack.com/](https://aviationstack.com/) to get your API key.  
Replace the placeholder in `etl/extract.py` with your key.

---

## 📚 Manual Execution

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

python etl/extract.py
python etl/transform_daily_traffic.py
python etl/load_epwa_daily_traffic.py
```

---

## 🧪 Airflow DAG

The DAG `epwa_flights_pipeline` runs daily and contains these tasks:

- `extract_from_api`
- `transform_daily_traffic`
- `transform_detailed_flights`
- `load_daily_traffic`
- `load_detailed_flights`

To run manually:
```bash
airflow db init
airflow scheduler
airflow webserver
```


---

## \ud83d\udc04 Updating the Project

Use the `update_project.sh` script to keep your local repository in sync with GitHub. The script checks for new commits on the tracked remote branch and pulls them if available.

```bash
./update_project.sh
```

You can add this script to a cron job or run it manually whenever you want to ensure you have the latest version.

---

## 📄 License

This project is licensed under the MIT License.

