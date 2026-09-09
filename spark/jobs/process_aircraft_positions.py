import argparse
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    broadcast,
    col,
    lit,
    row_number,
    substring,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window


# ============================================================
# SCHEMAS
# ============================================================

AIRCRAFT_SCHEMA = StructType(
    [
        StructField("icao24", StringType(), True),
        StructField("callsign", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("altitude", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ]
)

AIRLINE_SCHEMA = StructType(
    [
        StructField("airline_code", StringType(), True),
        StructField("airline_name", StringType(), True),
    ]
)


# ============================================================
# ARGUMENTS
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Traitement des positions avions"
    )

    parser.add_argument(
        "--input",
        default="data/raw/aircraft_positions.jsonl",
        help="Chemin du fichier JSONL RAW à traiter",
    )

    parser.add_argument(
        "--processing-date",
        default=None,
        help="Date de traitement au format YYYY-MM-DD",
    )

    return parser.parse_args()


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Toulouse Aviation Data Platform")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


# ============================================================
# DATA QUALITY
# ============================================================

def add_quality_checks(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "rejection_reason",
        when(
            col("icao24").isNull(),
            lit("MISSING_ICAO24"),
        )
        .when(
            col("timestamp").isNull(),
            lit("MISSING_TIMESTAMP"),
        )
        .when(
            col("ingestion_timestamp").isNull(),
            lit("MISSING_INGESTION_TIMESTAMP"),
        )
        .when(
            col("latitude").isNull(),
            lit("INVALID_OR_MISSING_LATITUDE"),
        )
        .when(
            (col("latitude") < -90)
            | (col("latitude") > 90),
            lit("INVALID_LATITUDE"),
        )
        .when(
            col("longitude").isNull(),
            lit("INVALID_OR_MISSING_LONGITUDE"),
        )
        .when(
            (col("longitude") < -180)
            | (col("longitude") > 180),
            lit("INVALID_LONGITUDE"),
        )
        .when(
            col("altitude").isNull(),
            lit("INVALID_OR_MISSING_ALTITUDE"),
        )
        .when(
            col("altitude") < 0,
            lit("ALTITUDE_BELOW_ZERO"),
        ),
    )


def split_valid_rejected(
    df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    valid_df = (
        df
        .filter(col("rejection_reason").isNull())
        .drop("rejection_reason")
    )

    rejected_df = df.filter(
        col("rejection_reason").isNotNull()
    )

    return valid_df, rejected_df


# ============================================================
# DEDUPLICATION
# ============================================================

def deduplicate_positions(df: DataFrame) -> DataFrame:
    window_spec = (
        Window
        .partitionBy(
            "icao24",
            "timestamp",
        )
        .orderBy(
            col("ingestion_timestamp").desc()
        )
    )

    return (
        df
        .withColumn(
            "row_num",
            row_number().over(window_spec),
        )
        .filter(col("row_num") == 1)
        .drop("row_num")
    )


# ============================================================
# ENRICHMENT
# ============================================================

def enrich_positions(
    df_positions: DataFrame,
    df_airlines: DataFrame,
) -> DataFrame:
    positions_with_airline_code = (
        df_positions
        .withColumn(
            "airline_code",
            substring(
                col("callsign"),
                1,
                3,
            ),
        )
    )

    return (
        positions_with_airline_code
        .join(
            broadcast(df_airlines),
            on="airline_code",
            how="left",
        )
    )


# ============================================================
# OUTPUT PATHS
# ============================================================

def build_output_paths(processing_date: str | None):
    if processing_date is None:
        return (
            Path("data/processed/aircraft_positions"),
            Path("data/rejected/aircraft_positions"),
        )

    date = datetime.strptime(
        processing_date,
        "%Y-%m-%d",
    )

    partition = (
        Path(f"year={date.year}")
        / f"month={date.month:02d}"
        / f"day={date.day:02d}"
    )

    processed_path = (
        Path("data/processed/aircraft_positions")
        / partition
    )

    rejected_path = (
        Path("data/rejected/aircraft_positions")
        / partition
    )

    return processed_path, rejected_path


# ============================================================
# MAIN
# ============================================================

def main():
    args = parse_args()

    spark = create_spark_session()

    print("=" * 60)
    print("TOULOUSE AVIATION DATA PLATFORM - PYSPARK")
    print("=" * 60)

    print(f"Lecture RAW : {args.input}")
    print(f"Processing date : {args.processing_date}")

    # --------------------------------------------------------
    # READ RAW
    # --------------------------------------------------------

    df_raw = (
        spark.read
        .schema(AIRCRAFT_SCHEMA)
        .json(args.input)
    )

    raw_count = df_raw.count()

    print(f"RAW : {raw_count}")

    # --------------------------------------------------------
    # DATA QUALITY
    # --------------------------------------------------------

    df_checked = add_quality_checks(df_raw)

    df_valid, df_rejected = split_valid_rejected(
        df_checked
    )

    valid_before_dedup_count = df_valid.count()
    rejected_count = df_rejected.count()

    print(
        "VALID BEFORE DEDUP : "
        f"{valid_before_dedup_count}"
    )

    print(
        "REJECTED : "
        f"{rejected_count}"
    )

    # --------------------------------------------------------
    # DEDUPLICATION
    # --------------------------------------------------------

    df_deduplicated = deduplicate_positions(
        df_valid
    )

    valid_after_dedup_count = (
        df_deduplicated.count()
    )

    duplicates_removed = (
        valid_before_dedup_count
        - valid_after_dedup_count
    )

    print(
        "DUPLICATES REMOVED : "
        f"{duplicates_removed}"
    )

    print(
        "VALID AFTER DEDUP : "
        f"{valid_after_dedup_count}"
    )

    # --------------------------------------------------------
    # READ AIRLINE REFERENCE
    # --------------------------------------------------------

    df_airlines = (
        spark.read
        .option("header", True)
        .schema(AIRLINE_SCHEMA)
        .csv("data/reference/airlines.csv")
    )

    # --------------------------------------------------------
    # ENRICHMENT
    # --------------------------------------------------------

    df_enriched = enrich_positions(
        df_deduplicated,
        df_airlines,
    )

    print()
    print("DONNEES VALIDES ENRICHIES")
    print("-" * 60)

    df_enriched.show(
        truncate=False
    )

    print()
    print("DONNEES REJETEES")
    print("-" * 60)

    df_rejected.show(
        truncate=False
    )

    # --------------------------------------------------------
    # PHYSICAL PLAN
    # --------------------------------------------------------

    print()
    print("PHYSICAL PLAN")
    print("-" * 60)

    df_enriched.explain()

    # --------------------------------------------------------
    # OUTPUT PATHS
    # --------------------------------------------------------

    processed_path, rejected_path = build_output_paths(
        args.processing_date
    )

    print()
    print(f"Processed path : {processed_path}")
    print(f"Rejected path : {rejected_path}")

    # --------------------------------------------------------
    # WRITE PROCESSED DATA
    # --------------------------------------------------------

    (
        df_enriched
        .write
        .mode("overwrite")
        .parquet(str(processed_path))
    )

    (
        df_rejected
        .write
        .mode("overwrite")
        .parquet(str(rejected_path))
    )

    print()
    print(
        f"Données processed écrites dans : "
        f"{processed_path}"
    )

    print(
        f"Données rejected écrites dans : "
        f"{rejected_path}"
    )

    # --------------------------------------------------------
    # READ BACK
    # --------------------------------------------------------

    print()
    print("LECTURE DES DONNEES PROCESSED")
    print("-" * 60)

    (
        spark.read
        .parquet(str(processed_path))
        .show(truncate=False)
    )

    print()
    print("LECTURE DES DONNEES REJECTED")
    print("-" * 60)

    (
        spark.read
        .parquet(str(rejected_path))
        .show(truncate=False)
    )

    spark.stop()


# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    main()