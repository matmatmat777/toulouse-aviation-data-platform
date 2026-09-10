from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


PROJECT_DIR = Path(__file__).resolve().parents[2]

INPUT_PATH = (
    PROJECT_DIR
    / "data"
    / "raw"
    / "aircraft_positions.jsonl"
)

DELTA_PATH = (
    PROJECT_DIR
    / "data"
    / "delta"
    / "bronze"
    / "aircraft_positions"
)


builder = (
    SparkSession.builder
    .appName("aviation-delta-bronze-demo")
    .master("local[2]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension",
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(
    builder
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Spark version:", spark.version)
print("Input:", INPUT_PATH)
print("Delta output:", DELTA_PATH)

df = spark.read.json(
    str(INPUT_PATH)
)

print("\n=== RAW DATA ===")
df.show(
    truncate=False
)

print("\n=== SCHEMA ===")
df.printSchema()

df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(str(DELTA_PATH))

print("\n=== DELTA TABLE CREATED ===")

delta_df = (
    spark.read
    .format("delta")
    .load(str(DELTA_PATH))
)

delta_df.show(
    truncate=False
)

print("\n=== CREATION VERSION 1 ===")

new_data = [
    {
        "altitude": "11000.0",
        "callsign": "EZY789",
        "icao24": "400abc",
        "ingestion_timestamp": "2026-09-03T13:06:05",
        "latitude": 43.65,
        "longitude": 1.50,
        "timestamp": "2026-09-03T13:06:00",
    },
    {
        "altitude": "12000.0",
        "callsign": "DLH321",
        "icao24": "3c1234",
        "ingestion_timestamp": "2026-09-03T13:07:05",
        "latitude": 43.70,
        "longitude": 1.55,
        "timestamp": "2026-09-03T13:07:00",
    },
    {
        "altitude": "9500.0",
        "callsign": "BAW555",
        "icao24": "406def",
        "ingestion_timestamp": "2026-09-03T13:08:05",
        "latitude": 43.75,
        "longitude": 1.60,
        "timestamp": "2026-09-03T13:08:00",
    },
]

new_df = spark.createDataFrame(new_data)

new_df.write \
    .format("delta") \
    .mode("append") \
    .save(str(DELTA_PATH))

print("3 lignes ajoutées.")

current_df = (
    spark.read
    .format("delta")
    .load(str(DELTA_PATH))
)

print("Nombre de lignes version courante :", current_df.count())

print("\n=== TIME TRAVEL ===")

version_0 = (
    spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .load(str(DELTA_PATH))
)

version_1 = (
    spark.read
    .format("delta")
    .option("versionAsOf", 1)
    .load(str(DELTA_PATH))
)

version_2 = (
    spark.read
    .format("delta")
    .option("versionAsOf", 2)
    .load(str(DELTA_PATH))
)

print("Version 0 :", version_0.count(), "lignes")
print("Version 1 :", version_1.count(), "lignes")
print("Version 2 :", version_2.count(), "lignes")

spark.stop()