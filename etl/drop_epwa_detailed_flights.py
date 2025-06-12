import duckdb
from pathlib import Path


def main():
    """Drop the epwa_detailed_flights table from DuckDB if it exists."""
    project_root = Path(__file__).resolve().parents[1]
    db_path = project_root / "db" / "epwa_traffic.duckdb"

    conn = duckdb.connect(str(db_path))

    try:
        conn.execute("DROP TABLE IF EXISTS epwa_detailed_flights")
        print("✔️ Table 'epwa_detailed_flights' has been dropped.")
    except Exception as e:
        print(f"❌ Error while dropping the table: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
