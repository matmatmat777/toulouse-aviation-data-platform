from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from spark.jobs.process_aircraft_positions import (
    AIRCRAFT_SCHEMA,
    AIRLINE_SCHEMA,
    add_quality_checks,
    split_valid_rejected,
    deduplicate_positions,
    enrich_positions,
)


# ============================================================
# Spark Session commune aux tests
# ============================================================

@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .master("local[2]")
        .appName("PySparkTests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark_session.sparkContext.setLogLevel("ERROR")

    yield spark_session

    spark_session.stop()


# ============================================================
# TEST 1
# Data Quality
# ============================================================

def test_quality_checks_reject_negative_altitude(spark):

    data = [
        (
            "bad001",
            "TEST01",
            43.6,
            1.4,
            -500.0,
            datetime(2026, 9, 3, 13, 0),
            datetime(2026, 9, 3, 13, 0, 5),
        )
    ]

    df = spark.createDataFrame(
        data,
        schema=AIRCRAFT_SCHEMA
    )

    result = add_quality_checks(df)

    row = result.first()

    assert row.rejection_reason == "ALTITUDE_BELOW_ZERO"


# ============================================================
# TEST 2
# Déduplication
# ============================================================

def test_dedup_keeps_latest_ingestion(spark):

    data = [
        (
            "39abcd",
            "AFR123",
            43.6047,
            1.4442,
            10000.0,
            datetime(2026, 9, 3, 13, 0),
            datetime(2026, 9, 3, 13, 0, 5),
        ),
        (
            "39abcd",
            "AFR123",
            43.6047,
            1.4442,
            10100.0,
            datetime(2026, 9, 3, 13, 0),
            datetime(2026, 9, 3, 13, 0, 8),
        ),
    ]

    df = spark.createDataFrame(
        data,
        schema=AIRCRAFT_SCHEMA
    )

    checked = add_quality_checks(df)

    valid, _ = split_valid_rejected(checked)

    result = deduplicate_positions(valid)

    rows = result.collect()

    assert len(rows) == 1

    assert rows[0].altitude == 10100.0

    assert rows[0].ingestion_timestamp == datetime(
        2026, 9, 3, 13, 0, 8
    )


# ============================================================
# TEST 3
# Enrichissement compagnie aérienne
# ============================================================

def test_enrichment_adds_airline_name(spark):

    positions = [
        (
            "39abcd",
            "AFR123",
            43.6047,
            1.4442,
            10000.0,
            datetime(2026, 9, 3, 13, 0),
            datetime(2026, 9, 3, 13, 0, 5),
        )
    ]

    airlines = [
        (
            "AFR",
            "Air France FAUX"
        )
    ]

    df_positions = spark.createDataFrame(
        positions,
        schema=AIRCRAFT_SCHEMA
    )

    df_airlines = spark.createDataFrame(
        airlines,
        schema=AIRLINE_SCHEMA
    )

    checked = add_quality_checks(
        df_positions
    )

    valid, _ = split_valid_rejected(
        checked
    )

    deduplicated = deduplicate_positions(
        valid
    )

    result = enrich_positions(
        deduplicated,
        df_airlines
    )

    row = result.first()

    assert row.airline_code == "AFR"

    assert row.airline_name == "Air France"

