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
    compute_rejection_rate,
    compute_duplicate_rate,
    compute_volume_variation_rate,
    compute_freshness_minutes,
    get_rejection_status,
    get_duplicate_status,
    get_volume_status,
    get_freshness_status,
    get_global_quality_status,
    enforce_quality_gate,
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
            "Air France"
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

def test_quality_gate_fails_when_global_status_failed():
    with pytest.raises(RuntimeError):
        enforce_quality_gate("FAILED")


def test_quality_gate_allows_warning():
    enforce_quality_gate("WARNING")

def test_rejection_rate_and_status():
    rate = compute_rejection_rate(
        raw_count=100,
        rejected_count=25,
    )

    assert rate == 25.0
    assert get_rejection_status(rate) == "FAILED"


def test_duplicate_rate_and_status():
    rate = compute_duplicate_rate(
        valid_before_dedup=100,
        duplicates_removed=5,
    )

    assert rate == 5.0
    assert get_duplicate_status(rate) == "WARNING"


def test_volume_variation_and_status():
    rate = compute_volume_variation_rate(
        expected_count=100,
        actual_count=75,
    )

    assert rate == -25.0
    assert get_volume_status(rate) == "FAILED"


def test_freshness_and_status():
    reference_time = datetime.fromisoformat(
        "2026-09-03T13:30:00"
    )

    latest_ingestion = datetime.fromisoformat(
        "2026-09-03T13:05:00"
    )

    freshness = compute_freshness_minutes(
        reference_time,
        latest_ingestion,
    )

    assert freshness == 25.0
    assert get_freshness_status(freshness) == "OK"


def test_freshness_failed():
    reference_time = datetime.fromisoformat(
        "2026-09-03T16:00:00"
    )

    latest_ingestion = datetime.fromisoformat(
        "2026-09-03T13:00:00"
    )

    freshness = compute_freshness_minutes(
        reference_time,
        latest_ingestion,
    )

    assert freshness == 180.0
    assert get_freshness_status(freshness) == "FAILED"


def test_global_quality_status_failed_has_priority():
    statuses = [
        "OK",
        "WARNING",
        "FAILED",
        "OK",
    ]

    assert get_global_quality_status(statuses) == "FAILED"


def test_global_quality_status_warning():
    statuses = [
        "OK",
        "WARNING",
        "OK",
    ]

    assert get_global_quality_status(statuses) == "WARNING"


def test_global_quality_status_ok():
    statuses = [
        "OK",
        "OK",
        "OK",
    ]

    assert get_global_quality_status(statuses) == "OK"

def test_future_timestamp_freshness_failed():
    freshness_minutes = -95.08

    assert (
        get_freshness_status(
            freshness_minutes
        )
        == "FAILED"
    )
