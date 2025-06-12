import duckdb
from pathlib import Path

# Path to the DuckDB database
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "db" / "epwa_traffic.duckdb"

# Connection to the database
conn = duckdb.connect(str(db_path))

# Dropping the table if it exists
try:
    conn.execute("DROP TABLE IF EXISTS epwa_detailed_flights")
    print("✔️ Table 'epwa_detailed_flights' has been dropped.")
except Exception as e:
    print(f"❌ Error while dropping the table: {e}")
    raise
finally:
    conn.close()
