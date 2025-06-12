from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pathlib import Path
from datetime import datetime


def main():
    """Transform raw arrival and departure JSON into a detailed CSV file."""
    spark = SparkSession.builder.appName("FlightDataDetailsSQL").getOrCreate()

    project_root = Path(__file__).resolve().parents[1]
    raw_dir = project_root / "data" / "raw"
    details_dir = project_root / "data" / "processed" / "detailed_flights"
    details_dir.mkdir(parents=True, exist_ok=True)

    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    dep_file = raw_dir / f"flights_dep_{today_str}.json"
    arr_file = raw_dir / f"flights_arr_{today_str}.json"

    dep_df_raw = spark.read.option("multiline", "true").json(str(dep_file))
    dep_df = dep_df_raw.select(explode("data").alias("flight_data"))
    dep_df.createOrReplaceTempView("dep_flights")

    arr_df_raw = spark.read.option("multiline", "true").json(str(arr_file))
    arr_df = arr_df_raw.select(explode("data").alias("flight_data"))
    arr_df.createOrReplaceTempView("arr_flights")

    dep_sql = """
        SELECT
            flight_data.flight_date AS flight_date,
            flight_data.flight_status AS flight_status,
            flight_data.departure.airport AS dep_airport,
            flight_data.departure.iata AS dep_iata,
            flight_data.departure.scheduled AS dep_scheduled,
            flight_data.departure.actual AS dep_actual,
            flight_data.departure.terminal AS dep_terminal,
            flight_data.departure.gate AS dep_gate,
            flight_data.departure.delay AS dep_delay,
            flight_data.arrival.airport AS arr_airport,
            flight_data.arrival.iata AS arr_iata,
            flight_data.arrival.scheduled AS arr_scheduled,
            flight_data.arrival.actual AS arr_actual,
            flight_data.arrival.terminal AS arr_terminal,
            flight_data.arrival.gate AS arr_gate,
            flight_data.arrival.delay AS arr_delay,
            flight_data.airline.name AS airline_name,
            flight_data.flight.number AS flight_number,
            'departure' AS operation_type,
            flight_data.departure.scheduled AS scheduled_datetime
        FROM dep_flights
    """

    arr_sql = """
        SELECT
            flight_data.flight_date AS flight_date,
            flight_data.flight_status AS flight_status,
            flight_data.departure.airport AS dep_airport,
            flight_data.departure.iata AS dep_iata,
            flight_data.departure.scheduled AS dep_scheduled,
            flight_data.departure.actual AS dep_actual,
            flight_data.departure.terminal AS dep_terminal,
            flight_data.departure.gate AS dep_gate,
            flight_data.departure.delay AS dep_delay,
            flight_data.arrival.airport AS arr_airport,
            flight_data.arrival.iata AS arr_iata,
            flight_data.arrival.scheduled AS arr_scheduled,
            flight_data.arrival.actual AS arr_actual,
            flight_data.arrival.terminal AS arr_terminal,
            flight_data.arrival.gate AS arr_gate,
            flight_data.arrival.delay AS arr_delay,
            flight_data.airline.name AS airline_name,
            flight_data.flight.number AS flight_number,
            'arrival' AS operation_type,
            flight_data.arrival.scheduled AS scheduled_datetime
        FROM arr_flights
    """

    dep_details = spark.sql(dep_sql)
    arr_details = spark.sql(arr_sql)
    detailed_df = dep_details.union(arr_details)
    detailed_df = detailed_df.filter(
        "flight_number IS NOT NULL AND scheduled_datetime IS NOT NULL"
    )

    output_path = str(details_dir / f"details_{today_str}_csv")
    detailed_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

    spark.stop()


if __name__ == "__main__":
    main()
