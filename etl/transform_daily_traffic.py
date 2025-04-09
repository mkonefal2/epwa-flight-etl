from pyspark.sql import SparkSession
from pathlib import Path
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("FlightDataDailySQL").getOrCreate()

# Paths
project_root = Path(__file__).resolve().parents[1]
raw_dir = project_root / "data" / "raw"
daily_dir = project_root / "data" / "processed" / "daily_traffic"
daily_dir.mkdir(parents=True, exist_ok=True)

today_str = datetime.utcnow().strftime('%Y-%m-%d')
dep_file = raw_dir / f"flights_dep_{today_str}.json"
arr_file = raw_dir / f"flights_arr_{today_str}.json"

# Load JSON and create temporary views
dep_df_raw = spark.read.option("multiline", "true").json(str(dep_file))
dep_df_raw.selectExpr("explode(data) as flight_data").createOrReplaceTempView("dep_flights")

arr_df_raw = spark.read.option("multiline", "true").json(str(arr_file))
arr_df_raw.selectExpr("explode(data) as flight_data").createOrReplaceTempView("arr_flights")

# SQL to aggregate departure data by date and hour
dep_sql = """
    SELECT
        to_date(flight_data.departure.scheduled) AS date,
        hour(flight_data.departure.scheduled) AS hour,
        COUNT(*) AS flights_count,
        'departure' AS operation_type
    FROM dep_flights
    WHERE flight_data.departure.scheduled IS NOT NULL
    GROUP BY to_date(flight_data.departure.scheduled), hour(flight_data.departure.scheduled)
"""

# SQL to aggregate arrival data by date and hour
arr_sql = """
    SELECT
        to_date(flight_data.arrival.scheduled) AS date,
        hour(flight_data.arrival.scheduled) AS hour,
        COUNT(*) AS flights_count,
        'arrival' AS operation_type
    FROM arr_flights
    WHERE flight_data.arrival.scheduled IS NOT NULL
    GROUP BY to_date(flight_data.arrival.scheduled), hour(flight_data.arrival.scheduled)
"""

# Execute SQL queries
dep_df = spark.sql(dep_sql)
arr_df = spark.sql(arr_sql)

# Combine and save to CSV
final_df = dep_df.union(arr_df)
output_path = str(daily_dir / f"daily_traffic_{today_str}_csv")
final_df.coalesce(1).write.mode('overwrite').csv(output_path, header=True)

spark.stop()
