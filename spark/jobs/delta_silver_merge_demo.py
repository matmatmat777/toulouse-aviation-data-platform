"""
TP DELTA LAKE - MERGE INCREMENTAL DANS SILVER
==============================================

Objectif :
Simuler l'arrivée d'un nouveau micro-batch contenant :

1. une correction d'un événement existant ;
2. un nouvel événement.

Nous allons utiliser MERGE pour :
- UPDATE si la clé existe ;
- INSERT si la clé n'existe pas.

Clé métier :
    (icao24, timestamp)
"""

from pathlib import Path

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)

from datetime import datetime


# ============================================================
# 1. CHEMIN SILVER
# ============================================================

PROJECT_DIR = Path(__file__).resolve().parents[2]

SILVER_PATH = (
    PROJECT_DIR
    / "data"
    / "delta"
    / "silver"
    / "aircraft_positions"
)


# ============================================================
# 2. SESSION SPARK + DELTA
# ============================================================

builder = (
    SparkSession.builder
    .appName("aviation-delta-silver-merge-demo")
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


# ============================================================
# 3. SCHEMA DE LA SOURCE INCREMENTALE
# ============================================================

"""
Contrairement à Bronze, notre source est ici déjà passée
par la logique de transformation Silver.

On utilise donc directement les bons types.
"""

schema = StructType([
    StructField("altitude", DoubleType(), True),
    StructField("callsign", StringType(), True),
    StructField("icao24", StringType(), False),
    StructField("ingestion_timestamp", TimestampType(), False),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("timestamp", TimestampType(), False),
    StructField("aircraft_type", StringType(), True),
])


# ============================================================
# 4. NOUVEAU MICRO-BATCH
# ============================================================

"""
Ligne 1 :
400abc existe déjà dans Silver.

Ancienne altitude :
    11500

Nouvelle altitude :
    11700

=> UPDATE


Ligne 2 :
new888 n'existe pas.

=> INSERT
"""

incremental_data = [

    (
        11700.0,
        "EZY789",
        "400abc",
        datetime.fromisoformat("2026-09-03T13:12:05"),
        43.67,
        1.52,
        datetime.fromisoformat("2026-09-03T13:06:00"),
        "A320",
    ),

    (
        10200.0,
        "AFR888",
        "new888",
        datetime.fromisoformat("2026-09-03T13:11:05"),
        43.85,
        1.75,
        datetime.fromisoformat("2026-09-03T13:11:00"),
        "A320",
    ),
]


source_df = spark.createDataFrame(
    incremental_data,
    schema=schema,
)


print("\n=== MICRO-BATCH SOURCE ===")

source_df.show(
    truncate=False
)


# ============================================================
# 5. TABLE DELTA SILVER
# ============================================================

silver_table = DeltaTable.forPath(
    spark,
    str(SILVER_PATH),
)


print("\n=== SILVER AVANT MERGE ===")

silver_table.toDF().orderBy(
    "timestamp"
).show(
    truncate=False
)

print(
    "Nombre de lignes avant MERGE :",
    silver_table.toDF().count()
)


# ============================================================
# 6. MERGE
# ============================================================

"""
La condition détermine si l'événement existe déjà.

Nous considérons qu'un événement est identifié par :

    icao24 + timestamp

Exemple :

400abc + 13:06
        ↓
déjà présent
        ↓
UPDATE


new888 + 13:11
        ↓
absent
        ↓
INSERT
"""

(
    silver_table.alias("target")

    .merge(
        source_df.alias("source"),

        """
        target.icao24 = source.icao24
        AND target.timestamp = source.timestamp
        """
    )

    .whenMatchedUpdateAll()

    .whenNotMatchedInsertAll()

    .execute()
)


# ============================================================
# 7. RESULTAT
# ============================================================

result_df = (
    spark.read
    .format("delta")
    .load(str(SILVER_PATH))
)


print("\n=== SILVER APRES MERGE ===")

result_df.orderBy(
    "timestamp"
).show(
    truncate=False
)


print(
    "Nombre de lignes après MERGE :",
    result_df.count()
)


# ============================================================
# 8. VERIFICATION DE LA CORRECTION
# ============================================================

print("\n=== VERIFICATION 400abc ===")

result_df.filter(
    "icao24 = '400abc'"
).show(
    truncate=False
)


print("\n=== VERIFICATION new888 ===")

result_df.filter(
    "icao24 = 'new888'"
).show(
    truncate=False
)


# ============================================================
# 9. HISTORIQUE DELTA
# ============================================================

"""
Delta conserve la transaction MERGE dans son _delta_log.

history() nous permet d'avoir une vue lisible de cet historique.
"""

print("\n=== HISTORIQUE DELTA SILVER ===")

silver_table.history().select(
    "version",
    "timestamp",
    "operation",
    "operationParameters",
).show(
    truncate=False
)


spark.stop()