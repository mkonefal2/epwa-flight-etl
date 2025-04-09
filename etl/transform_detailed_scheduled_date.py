from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit
from pathlib import Path
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("FlightDataDetails").getOrCreate()

project_root = Path(__file__).resolve().parents[1]
raw_dir = project_root / "data" / "raw"
details_dir = project_root / "data" / "processed" / "detailed_flights"
details_dir.mkdir(parents=True, exist_ok=True)

today_str = datetime.utcnow().strftime('%Y-%m-%d')
dep_file = raw_dir / f"flights_dep_{today_str}.json"
arr_file = raw_dir / f"flights_arr_{today_str}.json"

def extract_details(path, operation_type):
    df_raw = spark.read.option("multiline", "true").json(str(path))
    df = df_raw.selectExpr("explode(data) as flight_data")

    df_selected = df.select(
        col("flight_data.flight_date").alias("flight_date"),
        col("flight_data.flight_status").alias("flight_status"),
        col("flight_data.departure.airport").alias("dep_airport"),
        col("flight_data.departure.scheduled").alias("dep_scheduled"),
        col("flight_data.departure.actual").alias("dep_actual"),
        col("flight_data.departure.terminal").alias("dep_terminal"),
        col("flight_data.departure.gate").alias("dep_gate"),
        col("flight_data.departure.delay").alias("dep_delay"),
        col("flight_data.arrival.airport").alias("arr_airport"),
        col("flight_data.arrival.scheduled").alias("arr_scheduled"),
        col("flight_data.arrival.actual").alias("arr_actual"),
        col("flight_data.arrival.terminal").alias("arr_terminal"),
        col("flight_data.arrival.gate").alias("arr_gate"),
        col("flight_data.arrival.delay").alias("arr_delay"),
        col("flight_data.airline.name").alias("airline_name"),
        col("flight_data.flight.number").alias("flight_number"),
        lit(operation_type).alias("operation_type"),
        col(f"flight_data.{operation_type}.scheduled").alias("scheduled_datetime")
    )
    return df_selected

# Przetwarzanie szczegółowych danych
dep_details = extract_details(dep_file, "departure")
arr_details = extract_details(arr_file, "arrival")

detailed_df = dep_details.union(arr_details)

# Usuń wiersze bez flight_number lub scheduled_datetime
detailed_df = detailed_df.filter(
    (col("flight_number").isNotNull()) & (col("scheduled_datetime").isNotNull())
)



# Zapis do CSV
output_path = str(details_dir / f"details_{today_str}_csv")
detailed_df.coalesce(1).write.mode('overwrite').csv(output_path, header=True)

spark.stop()
