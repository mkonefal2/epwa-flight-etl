import duckdb
from datetime import datetime
from pathlib import Path
import sys


def main():
    """Create a dated backup of the detailed flights table."""
    project_root = Path(__file__).resolve().parents[1]
    db_path = project_root / "db" / "epwa_traffic.duckdb"

    today_str = datetime.today().strftime("%Y_%m_%d")
    backup_table = f"epwa_detailed_flights_backup_{today_str}"

    conn = duckdb.connect(str(db_path))

    tables = conn.execute("SHOW TABLES").fetchall()
    table_names = [t[0] for t in tables]

    if backup_table in table_names:
        print(f"⚠️ Backup {backup_table} already exists. Skipping creation.")
    else:
        print(f"🛠 Creating backup: {backup_table}")
        conn.execute(f"CREATE TABLE {backup_table} AS SELECT * FROM epwa_detailed_flights")

    try:
        result = conn.execute(f"SELECT COUNT(*) FROM {backup_table}").fetchone()
        row_count = result[0]

        if row_count > 0:
            print(f"✔️ Backup {backup_table} contains {row_count} records — OK.")
        else:
            print(f"❌ Table {backup_table} was created but is empty!")
            sys.exit(1)

    except Exception as e:
        print(f"❌ Error during backup validation: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
