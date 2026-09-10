import argparse
import json
import logging

from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    broadcast,
    col,
    lit,
    max as spark_max,
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
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s "
        "%(levelname)s "
        "%(name)s - "
        "%(message)s"
    ),
)

logger = logging.getLogger(
    "aviation-pyspark"
)


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

    parser.add_argument(
        "--expected-count",
        type=int,
        default=None,
        help="Volume de lignes attendu pour le traitement",
    )

    parser.add_argument(
        "--reference-time",
        default=None,
        help=(
            "Heure de référence ISO pour la freshness. "
            "Exemple : 2026-09-03T13:30:00"
        ),
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
# DATA QUALITY - ROW LEVEL
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

    rejected_df = (
        df
        .filter(col("rejection_reason").isNotNull())
    )

    return valid_df, rejected_df


# ============================================================
# DATA QUALITY - METRICS
# ============================================================

def compute_rejection_rate(
    raw_count,
    rejected_count,
):
    if raw_count <= 0:
        return 0.0

    return (
        rejected_count
        / raw_count
        * 100
    )


def compute_duplicate_rate(
    valid_before_dedup,
    duplicates_removed,
):
    if valid_before_dedup <= 0:
        return 0.0

    return (
        duplicates_removed
        / valid_before_dedup
        * 100
    )


def compute_volume_variation_rate(
    expected_count,
    actual_count,
):
    if (
        expected_count is None
        or expected_count <= 0
    ):
        return 0.0

    return (
        (actual_count - expected_count)
        / expected_count
        * 100
    )


def compute_freshness_minutes(
    reference_time,
    latest_ingestion_timestamp,
):
    if latest_ingestion_timestamp is None:
        return None

    freshness_delay = (
        reference_time
        - latest_ingestion_timestamp
    )

    return (
        freshness_delay.total_seconds()
        / 60
    )


# ============================================================
# DATA QUALITY - STATUS
# ============================================================

def get_rejection_status(
    rejection_rate,
):
    if rejection_rate <= 5:
        return "OK"

    elif rejection_rate <= 20:
        return "WARNING"

    else:
        return "FAILED"


def get_duplicate_status(
    duplicate_rate,
):
    if duplicate_rate <= 2:
        return "OK"

    elif duplicate_rate <= 10:
        return "WARNING"

    else:
        return "FAILED"


def get_volume_status(
    volume_variation_rate,
):
    absolute_variation = abs(
        volume_variation_rate
    )

    if absolute_variation <= 10:
        return "OK"

    elif absolute_variation <= 20:
        return "WARNING"

    else:
        return "FAILED"


def get_freshness_status(
    freshness_minutes,
):
    if freshness_minutes is None:
        return "FAILED"

    # Timestamp situé dans le futur par rapport
    # à notre heure de référence
    if freshness_minutes < 0:
        return "FAILED"

    if freshness_minutes <= 30:
        return "OK"

    elif freshness_minutes <= 120:
        return "WARNING"

    else:
        return "FAILED"


def get_global_quality_status(
    statuses,
):
    if "FAILED" in statuses:
        return "FAILED"

    elif "WARNING" in statuses:
        return "WARNING"

    else:
        return "OK"


# ============================================================
# METRICS PERSISTENCE
# ============================================================

def build_metrics_path(
    processing_date: str | None,
) -> Path:

    if processing_date is None:
        return (
            Path(
                "data/metrics/"
                "aircraft_positions/"
                "quality_metrics.json"
            )
        )

    date = datetime.strptime(
        processing_date,
        "%Y-%m-%d",
    )

    return (
        Path(
            "data/metrics/"
            "aircraft_positions"
        )
        / f"year={date.year}"
        / f"month={date.month:02d}"
        / f"day={date.day:02d}"
        / "quality_metrics.json"
    )


def persist_quality_metrics(
    metrics: dict,
    metrics_path: Path,
):
    metrics_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with metrics_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            metrics,
            file,
            indent=2,
            ensure_ascii=False,
        )

    logger.info(
        "Quality metrics persisted path=%s",
        metrics_path,
    )


# ============================================================
# DATA QUALITY GATE
# ============================================================

def enforce_quality_gate(
    global_status,
):
    if global_status == "FAILED":

        logger.error(
            "Data Quality Gate failed status=%s",
            global_status,
        )

        raise RuntimeError(
            "Data Quality Gate FAILED: "
            "critical data quality threshold exceeded"
        )

    logger.info(
        "Data Quality Gate passed status=%s",
        global_status,
    )


# ============================================================
# DEDUPLICATION
# ============================================================

def deduplicate_positions(
    df: DataFrame,
) -> DataFrame:

    window_spec = (
        Window
        .partitionBy(
            "icao24",
            "timestamp",
        )
        .orderBy(
            col(
                "ingestion_timestamp"
            ).desc()
        )
    )

    return (
        df
        .withColumn(
            "row_num",
            row_number().over(
                window_spec
            ),
        )
        .filter(
            col("row_num") == 1
        )
        .drop(
            "row_num"
        )
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
            broadcast(
                df_airlines
            ),
            on="airline_code",
            how="left",
        )
    )


# ============================================================
# OUTPUT PATHS
# ============================================================

def build_output_paths(
    processing_date: str | None,
):
    if processing_date is None:
        return (
            Path(
                "data/processed/"
                "aircraft_positions"
            ),
            Path(
                "data/rejected/"
                "aircraft_positions"
            ),
        )

    date = datetime.strptime(
        processing_date,
        "%Y-%m-%d",
    )

    partition = (
        Path(
            f"year={date.year}"
        )
        / f"month={date.month:02d}"
        / f"day={date.day:02d}"
    )

    processed_path = (
        Path(
            "data/processed/"
            "aircraft_positions"
        )
        / partition
    )

    rejected_path = (
        Path(
            "data/rejected/"
            "aircraft_positions"
        )
        / partition
    )

    return (
        processed_path,
        rejected_path,
    )


# ============================================================
# MAIN
# ============================================================

def main():
    args = parse_args()

    spark = create_spark_session()

    logger.info(
        "Job started input=%s processing_date=%s "
        "expected_count=%s reference_time=%s",
        args.input,
        args.processing_date,
        args.expected_count,
        args.reference_time,
    )

    # --------------------------------------------------------
    # READ RAW
    # --------------------------------------------------------

    df_raw = (
        spark.read
        .schema(
            AIRCRAFT_SCHEMA
        )
        .json(
            args.input
        )
    )

    raw_count = (
        df_raw.count()
    )

    logger.info(
        "RAW loaded raw_count=%s",
        raw_count,
    )

    # --------------------------------------------------------
    # FRESHNESS
    # --------------------------------------------------------

    latest_ingestion_timestamp = (
        df_raw
        .agg(
            spark_max(
                "ingestion_timestamp"
            ).alias(
                "latest_ingestion_timestamp"
            )
        )
        .first()[
            "latest_ingestion_timestamp"
        ]
    )

    freshness_minutes = None
    freshness_status = None

    if args.reference_time is not None:

        reference_time = (
            datetime.fromisoformat(
                args.reference_time
            )
        )

        freshness_minutes = (
            compute_freshness_minutes(
                reference_time,
                latest_ingestion_timestamp,
            )
        )

        freshness_status = (
            get_freshness_status(
                freshness_minutes
            )
        )

    # --------------------------------------------------------
    # ROW QUALITY CHECKS
    # --------------------------------------------------------

    df_checked = (
        add_quality_checks(
            df_raw
        )
    )

    (
        df_valid,
        df_rejected,
    ) = split_valid_rejected(
        df_checked
    )

    valid_before_dedup_count = (
        df_valid.count()
    )

    rejected_count = (
        df_rejected.count()
    )

    # --------------------------------------------------------
    # DEDUPLICATION
    # --------------------------------------------------------

    df_deduplicated = (
        deduplicate_positions(
            df_valid
        )
    )

    valid_after_dedup_count = (
        df_deduplicated.count()
    )

    duplicates_removed = (
        valid_before_dedup_count
        - valid_after_dedup_count
    )

    # --------------------------------------------------------
    # QUALITY METRICS
    # --------------------------------------------------------

    rejection_rate = (
        compute_rejection_rate(
            raw_count,
            rejected_count,
        )
    )

    duplicate_rate = (
        compute_duplicate_rate(
            valid_before_dedup_count,
            duplicates_removed,
        )
    )

    rejection_status = (
        get_rejection_status(
            rejection_rate
        )
    )

    duplicate_status = (
        get_duplicate_status(
            duplicate_rate
        )
    )

    # --------------------------------------------------------
    # VOLUME
    # --------------------------------------------------------

    volume_variation_rate = None
    volume_status = None

    if args.expected_count is not None:

        volume_variation_rate = (
            compute_volume_variation_rate(
                args.expected_count,
                raw_count,
            )
        )

        volume_status = (
            get_volume_status(
                volume_variation_rate
            )
        )

    # --------------------------------------------------------
    # GLOBAL STATUS
    # --------------------------------------------------------

    statuses = [
        rejection_status,
        duplicate_status,
    ]

    if volume_status is not None:
        statuses.append(
            volume_status
        )

    if freshness_status is not None:
        statuses.append(
            freshness_status
        )

    global_status = (
        get_global_quality_status(
            statuses
        )
    )

    # --------------------------------------------------------
    # OBSERVABILITY LOGS
    # --------------------------------------------------------

    logger.info(
        "Data quality metrics "
        "raw_count=%s "
        "valid_before_dedup=%s "
        "rejected_count=%s "
        "duplicates_removed=%s "
        "valid_after_dedup=%s",
        raw_count,
        valid_before_dedup_count,
        rejected_count,
        duplicates_removed,
        valid_after_dedup_count,
    )

    logger.info(
        "Data quality status "
        "rejection_rate=%.2f "
        "rejection_status=%s "
        "duplicate_rate=%.2f "
        "duplicate_status=%s "
        "global_status=%s",
        rejection_rate,
        rejection_status,
        duplicate_rate,
        duplicate_status,
        global_status,
    )

    if volume_status is not None:
        logger.info(
            "Volume metric expected_count=%s "
            "actual_count=%s "
            "variation_rate=%.2f "
            "status=%s",
            args.expected_count,
            raw_count,
            volume_variation_rate,
            volume_status,
        )

    if freshness_status is not None:
        logger.info(
            "Freshness metric "
            "latest_ingestion_timestamp=%s "
            "freshness_minutes=%.2f "
            "status=%s",
            latest_ingestion_timestamp,
            freshness_minutes,
            freshness_status,
        )

    # --------------------------------------------------------
    # BUILD METRICS OBJECT
    # --------------------------------------------------------

    metrics = {
        "processing_date": args.processing_date,
        "input": args.input,
        "raw_count": raw_count,
        "valid_before_dedup": (
            valid_before_dedup_count
        ),
        "rejected_count": rejected_count,
        "duplicates_removed": (
            duplicates_removed
        ),
        "valid_after_dedup": (
            valid_after_dedup_count
        ),
        "rejection_rate": round(
            rejection_rate,
            2,
        ),
        "duplicate_rate": round(
            duplicate_rate,
            2,
        ),
        "expected_count": (
            args.expected_count
        ),
        "volume_variation_rate": (
            round(
                volume_variation_rate,
                2,
            )
            if volume_variation_rate
            is not None
            else None
        ),
        "latest_ingestion_timestamp": (
            latest_ingestion_timestamp.isoformat()
            if latest_ingestion_timestamp
            is not None
            else None
        ),
        "freshness_minutes": (
            round(
                freshness_minutes,
                2,
            )
            if freshness_minutes
            is not None
            else None
        ),
        "rejection_status": (
            rejection_status
        ),
        "duplicate_status": (
            duplicate_status
        ),
        "volume_status": (
            volume_status
        ),
        "freshness_status": (
            freshness_status
        ),
        "global_status": (
            global_status
        ),
        "generated_at": (
            datetime.utcnow().isoformat()
        ),
    }

    # --------------------------------------------------------
    # PERSIST METRICS BEFORE QUALITY GATE
    # --------------------------------------------------------

    metrics_path = (
        build_metrics_path(
            args.processing_date
        )
    )

    persist_quality_metrics(
        metrics,
        metrics_path,
    )

    # --------------------------------------------------------
    # QUALITY GATE
    # --------------------------------------------------------

    enforce_quality_gate(
        global_status
    )

    # --------------------------------------------------------
    # READ AIRLINE REFERENCE
    # --------------------------------------------------------

    df_airlines = (
        spark.read
        .option(
            "header",
            True,
        )
        .schema(
            AIRLINE_SCHEMA
        )
        .csv(
            "data/reference/"
            "airlines.csv"
        )
    )

    # --------------------------------------------------------
    # ENRICHMENT
    # --------------------------------------------------------

    df_enriched = (
        enrich_positions(
            df_deduplicated,
            df_airlines,
        )
    )

    logger.info(
        "Airline enrichment completed"
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

    (
        processed_path,
        rejected_path,
    ) = build_output_paths(
        args.processing_date
    )

    # --------------------------------------------------------
    # WRITE PROCESSED
    # --------------------------------------------------------

    (
        df_enriched
        .write
        .mode(
            "overwrite"
        )
        .parquet(
            str(
                processed_path
            )
        )
    )

    logger.info(
        "Processed data written path=%s",
        processed_path,
    )

    # --------------------------------------------------------
    # WRITE REJECTED
    # --------------------------------------------------------

    (
        df_rejected
        .write
        .mode(
            "overwrite"
        )
        .parquet(
            str(
                rejected_path
            )
        )
    )

    logger.info(
        "Rejected data written path=%s",
        rejected_path,
    )

    spark.stop()

    logger.info(
        "Job completed successfully"
    )


# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    main()