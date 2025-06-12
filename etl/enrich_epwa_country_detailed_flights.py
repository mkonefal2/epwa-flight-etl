from pyspark.sql import SparkSession
import duckdb
from pathlib import Path
from datetime import datetime


def main():
    """Enrich detailed flights with arrival country using airports reference."""
    spark = SparkSession.builder.appName("EnrichArrivalCountry").getOrCreate()

    project_root = Path(__file__).resolve().parents[1]
    airports_path = project_root / "data" / "reference" / "airports.csv"
    db_path = project_root / "db" / "epwa_traffic.duckdb"

    today_str = datetime.utcnow().strftime("%Y-%m-%d")

    conn = duckdb.connect(str(db_path))
    df_flights = conn.execute(
        f"""
        SELECT flight_number, operation_type, scheduled_datetime, arr_iata
        FROM epwa_detailed_flights
        WHERE flight_date = '{today_str}'
        """
    ).df()
    df_flights = spark.createDataFrame(df_flights)
    df_flights.createOrReplaceTempView("flights")

    df_airports = spark.read.option("header", "true").csv(str(airports_path))
    df_airports.createOrReplaceTempView("airports")

    query = """
        SELECT
            f.flight_number,
            f.operation_type,
            f.scheduled_datetime,
            a.iso_country AS arrival_country
        FROM flights f
        LEFT JOIN airports a
            ON f.arr_iata = a.iata_code
        WHERE f.flight_number IS NOT NULL AND f.operation_type IS NOT NULL AND f.scheduled_datetime IS NOT NULL
    """

    df_enriched = spark.sql(query)

    conn.execute("ALTER TABLE epwa_detailed_flights ADD COLUMN IF NOT EXISTS arrival_country VARCHAR;")

    df_enriched_pd = df_enriched.toPandas()
    conn.register("enriched", df_enriched_pd)

    conn.execute(
        """
        UPDATE epwa_detailed_flights AS main
        SET arrival_country = sub.arrival_country
        FROM enriched AS sub
        WHERE
            main.flight_number = sub.flight_number AND
            main.operation_type = sub.operation_type AND
            main.scheduled_datetime = sub.scheduled_datetime
        """
    )

    print("✅ arrival_country updated directly in epwa_detailed_flights.")
    conn.close()
    spark.stop()


if __name__ == "__main__":
    main()
