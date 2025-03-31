import duckdb
import glob
from pathlib import Path

# 🔗 Ścieżki
project_root = Path(__file__).resolve().parents[1]
db_path = project_root / 'db' / 'epwa_traffic.duckdb'
csv_dirs = glob.glob(str(project_root / 'data' / 'processed' / 'daily_traffic' / 'daily_traffic_*_csv'))

# 🔌 Połączenie z DuckDB
conn = duckdb.connect(str(db_path))

# 📅 Tworzenie tabeli, jeśli nie istnieje
conn.execute('''
    CREATE TABLE IF NOT EXISTS epwa_daily_traffic (
        date DATE NOT NULL,
        hour INTEGER NOT NULL,
        operation_type VARCHAR NOT NULL,
        flights_count INTEGER NOT NULL,
        PRIMARY KEY (date, hour, operation_type)
    )
''')

# 📅 Załadowanie danych z plików CSV
for dir_path in csv_dirs:
    csv_files = glob.glob(f"{dir_path}/*.csv")
    for file in csv_files:
        print(f"🗕️ Załadowano plik: {file}")
        conn.execute(f'''
            INSERT INTO epwa_daily_traffic (date, hour, operation_type, flights_count)
            SELECT
                CAST(date AS DATE),
                CAST(hour AS INTEGER),
                CAST(operation_type AS VARCHAR),
                CAST(flights_count AS INTEGER)
            FROM read_csv_auto('{file}', HEADER=TRUE)
            WHERE date IS NOT NULL AND hour IS NOT NULL AND operation_type IS NOT NULL
            ON CONFLICT (date, hour, operation_type) DO UPDATE SET
                flights_count = EXCLUDED.flights_count
        ''')

print("✅ Załadowano dane do epwa_daily_traffic.")
conn.close()