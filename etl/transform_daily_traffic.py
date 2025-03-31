from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, hour, lit
from pathlib import Path
from datetime import datetime

# Inicjalizacja sesji Spark
spark = SparkSession.builder.appName("FlightDataDaily").getOrCreate()

project_root = Path(__file__).resolve().parents[1]
raw_dir = project_root / "data" / "raw"
daily_dir = project_root / "data" / "processed" / "daily_traffic"
daily_dir.mkdir(parents=True, exist_ok=True)

today_str = datetime.utcnow().strftime('%Y-%m-%d')
dep_file = raw_dir / f"flights_dep_{today_str}.json"
arr_file = raw_dir / f"flights_arr_{today_str}.json"

def process_file(path, operation_type):
    df_raw = spark.read.option("multiline", "true").json(str(path))

    # Rozwinięcie kolumny data i wybór właściwej kolumny scheduled
    df = df_raw.selectExpr("explode(data) as flight_data") \
        .select(
            to_date(col(f"flight_data.{operation_type}.scheduled")).alias("date"),
            hour(col(f"flight_data.{operation_type}.scheduled")).alias("hour")
        )

    # Agregacja liczby lotów na godzinę
    df_agg = df.groupBy("date", "hour") \
               .count() \
               .withColumnRenamed("count", "flights_count") \
               .withColumn("operation_type", lit(operation_type))

    return df_agg

# Przetwarzanie plików
dep_df = process_file(dep_file, "departure")
arr_df = process_file(arr_file, "arrival")

# Łączenie wyników
final_df = dep_df.union(arr_df)

# Zapis do CSV
output_path = str(daily_dir/ f"daily_traffic_{today_str}_csv")
final_df.coalesce(1).write.mode('overwrite').csv(output_path, header=True)

spark.stop()