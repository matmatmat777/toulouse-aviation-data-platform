from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


PROJECT_DIR = Path(__file__).resolve().parents[2]

DELTA_PATH = (
    PROJECT_DIR
    / "data"
    / "delta"
    / "bronze"
    / "aircraft_positions"
)

builder = (
    SparkSession.builder
    .appName("aviation-delta-time-travel")
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

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

for version in [0, 1, 2]:

    df = (
        spark.read
        .format("delta")
        .option("versionAsOf", version)
        .load(str(DELTA_PATH))
    )

    print(
        f"Version {version} : {df.count()} lignes"
    )

spark.stop()