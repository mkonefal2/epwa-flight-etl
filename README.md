# ✈️ EPWA Daily Flights ETL Pipeline

This project is an ETL pipeline that extracts, transforms, and loads data about flights related to the **Warsaw Chopin Airport (EPWA)** using the [Aviationstack API](https://aviationstack.com/). It stores and analyzes both detailed and aggregated flight traffic using **PySpark**, **DuckDB**, and **Apache Airflow**.

---

## 📦 Quick Start (Automated Setup)

You can set up the entire environment automatically on a fresh Ubuntu machine.

### 🔧 Installation Steps

1. Clone the official installer:
   ```bash
   git clone https://github.com/mkonefal2/airflow-flight-installer.git
   cd airflow-flight-installer
   ```

2. Download the latest ZIP version of this repository from [Releases](https://github.com/mkonefal2/epwa-flight-etl/releases).

3. Place the downloaded file (`epwa-flight-etl-main.zip`) in the same folder as `install_epwa-flight-etl.sh`.

4. Run the installer:
   ```bash
   chmod +x install_epwa-flight-etl.sh
   sudo ./install_epwa-flight-etl.sh
   ```

5. After a successful installation, visit:
   ```
   http://<your-vm-ip>:8080
   ```

   Default Airflow credentials:
   - **Username:** `admin`
   - **Password:** `StrongPassword123`

---

## 📁 Project Structure

```
project_epwa_daily_traffic/
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

## 📄 License

This project is licensed under the MIT License.

