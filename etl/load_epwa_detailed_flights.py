import duckdb
import glob
from pathlib import Path


def main():
    """Load detailed flight CSVs into DuckDB."""
    project_root = Path(__file__).resolve().parents[1]
    db_path = project_root / "db" / "epwa_traffic.duckdb"
    csv_dirs = glob.glob(
        str(project_root / "data" / "processed" / "detailed_flights" / "details_*_csv")
    )

    conn = duckdb.connect(str(db_path))

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS epwa_detailed_flights (
            flight_date DATE,
            flight_status VARCHAR,
            dep_airport VARCHAR,
            dep_iata VARCHAR,
            dep_scheduled TIMESTAMP,
            dep_actual TIMESTAMP,
            dep_terminal VARCHAR,
            dep_gate VARCHAR,
            dep_delay INTEGER,
            arr_airport VARCHAR,
            arr_iata VARCHAR,
            arr_scheduled TIMESTAMP,
            arr_actual TIMESTAMP,
            arr_terminal VARCHAR,
            arr_gate VARCHAR,
            arr_delay INTEGER,
            airline_name VARCHAR,
            flight_number VARCHAR NOT NULL,
            operation_type VARCHAR NOT NULL,
            scheduled_datetime TIMESTAMP NOT NULL,
            arrival_country VARCHAR,
            PRIMARY KEY (scheduled_datetime, flight_number, operation_type)
        )
        """
    )

    for dir_path in csv_dirs:
        csv_files = glob.glob(f"{dir_path}/*.csv")
        for file in csv_files:
            print(f"📅 Loading file: {file}")
            conn.execute(
                f"""
                INSERT INTO epwa_detailed_flights (
                    flight_date, flight_status, dep_airport, dep_iata,
                    dep_scheduled, dep_actual, dep_terminal, dep_gate, dep_delay,
                    arr_airport, arr_iata, arr_scheduled, arr_actual, arr_terminal, arr_gate, arr_delay,
                    airline_name, flight_number, operation_type, scheduled_datetime
                )
                SELECT
                    CAST(flight_date AS DATE),
                    flight_status,
                    dep_airport,
                    dep_iata,
                    CAST(dep_scheduled AS TIMESTAMP),
                    CAST(dep_actual AS TIMESTAMP),
                    dep_terminal,
                    dep_gate,
                    CAST(dep_delay AS INTEGER),
                    arr_airport,
                    arr_iata,
                    CAST(arr_scheduled AS TIMESTAMP),
                    CAST(arr_actual AS TIMESTAMP),
                    arr_terminal,
                    arr_gate,
                    CAST(arr_delay AS INTEGER),
                    airline_name,
                    flight_number,
                    operation_type,
                    CAST(scheduled_datetime AS TIMESTAMP)
                FROM read_csv_auto('{file}', HEADER=TRUE)
                WHERE flight_number IS NOT NULL AND operation_type IS NOT NULL AND scheduled_datetime IS NOT NULL
                ON CONFLICT(scheduled_datetime, flight_number, operation_type) DO UPDATE SET
                    flight_date=EXCLUDED.flight_date,
                    flight_status=EXCLUDED.flight_status,
                    dep_airport=EXCLUDED.dep_airport,
                    dep_iata=EXCLUDED.dep_iata,
                    dep_scheduled=EXCLUDED.dep_scheduled,
                    dep_actual=EXCLUDED.dep_actual,
                    dep_terminal=EXCLUDED.dep_terminal,
                    dep_gate=EXCLUDED.dep_gate,
                    dep_delay=EXCLUDED.dep_delay,
                    arr_airport=EXCLUDED.arr_airport,
                    arr_iata=EXCLUDED.arr_iata,
                    arr_scheduled=EXCLUDED.arr_scheduled,
                    arr_actual=EXCLUDED.arr_actual,
                    arr_terminal=EXCLUDED.arr_terminal,
                    arr_gate=EXCLUDED.arr_gate,
                    arr_delay=EXCLUDED.arr_delay,
                    airline_name=EXCLUDED.airline_name
                """
            )

    print("✅ Data loaded into epwa_detailed_flights.")
    conn.close()


if __name__ == "__main__":
    main()
