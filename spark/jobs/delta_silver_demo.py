"""
TP DELTA LAKE - BRONZE -> SILVER
================================

Objectif pédagogique :
Construire une vraie table Silver à partir de notre Bronze Delta.

BRONZE :
- proche de la source ;
- types encore imparfaits ;
- peut contenir des données invalides ;
- peut contenir des doublons.

SILVER :
- données typées ;
- nettoyées ;
- validées ;
- dédupliquées ;
- prêtes pour des traitements métier.

Dans notre cas :

altitude
    string -> double

timestamp
    string -> timestamp

ingestion_timestamp
    string -> timestamp

Les lignes invalides sont rejetées.

Les doublons sont supprimés en gardant la donnée
avec le dernier ingestion_timestamp.
"""

from pathlib import Path

from delta import configure_spark_with_delta_pip

from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    expr,
    row_number,
)

from pyspark.sql.window import Window


# ============================================================
# 1. CHEMINS DU PROJET
# ============================================================

PROJECT_DIR = Path(__file__).resolve().parents[2]


# Table Bronze existante
BRONZE_PATH = (
    PROJECT_DIR
    / "data"
    / "delta"
    / "bronze"
    / "aircraft_positions"
)


# Nouvelle table Silver
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
    .appName("aviation-delta-silver-demo")
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
# 3. LECTURE DU BRONZE
# ============================================================

bronze_df = (
    spark.read
    .format("delta")
    .load(str(BRONZE_PATH))
)


print("\n=== BRONZE ===")

bronze_df.show(
    truncate=False
)

print("\n=== SCHEMA BRONZE ===")

bronze_df.printSchema()


# ============================================================
# 4. TYPAGE DES DONNEES
# ============================================================

"""
Notre Bronze contient par exemple :

    altitude = "10000.0"
    altitude = "PAS_UN_NOMBRE"

Nous utilisons try_cast :

    try_cast(altitude AS DOUBLE)

Contrairement à un cast strict, try_cast produit NULL
lorsque la conversion est impossible.

Exemple :

    "10000.0"       -> 10000.0
    "PAS_UN_NOMBRE" -> NULL

Nous pourrons ensuite détecter ce NULL via nos règles DQ.
"""

typed_df = (
    bronze_df

    .withColumn(
        "altitude",
        expr("try_cast(altitude AS DOUBLE)")
    )

    .withColumn(
        "timestamp",
        expr("try_cast(timestamp AS TIMESTAMP)")
    )

    .withColumn(
        "ingestion_timestamp",
        expr(
            "try_cast(ingestion_timestamp AS TIMESTAMP)"
        )
    )
)


print("\n=== APRES TYPAGE ===")

typed_df.show(
    truncate=False
)

print("\n=== SCHEMA APRES TYPAGE ===")

typed_df.printSchema()


# ============================================================
# 5. DATA QUALITY
# ============================================================

"""
Nous appliquons ici quelques règles simples :

icao24 :
    obligatoire

timestamp :
    obligatoire

ingestion_timestamp :
    obligatoire

latitude :
    obligatoire
    entre -90 et 90

longitude :
    obligatoire
    entre -180 et 180

altitude :
    obligatoire
    >= 0
"""

valid_df = (
    typed_df

    .filter(
        col("icao24").isNotNull()
    )

    .filter(
        col("timestamp").isNotNull()
    )

    .filter(
        col("ingestion_timestamp").isNotNull()
    )

    .filter(
        col("latitude").isNotNull()
    )

    .filter(
        col("longitude").isNotNull()
    )

    .filter(
        col("altitude").isNotNull()
    )

    .filter(
        col("latitude").between(-90, 90)
    )

    .filter(
        col("longitude").between(-180, 180)
    )

    .filter(
        col("altitude") >= 0
    )
)


print("\n=== DONNEES VALIDES AVANT DEDUP ===")

valid_df.show(
    truncate=False
)


# ============================================================
# 6. DEDUPLICATION
# ============================================================

"""
Dans notre jeu de données, nous avons un doublon logique :

    icao24 = 39abcd
    timestamp = 2026-09-03 13:00:00

Deux lignes existent pour ce même événement.

Nous voulons garder celle qui a été ingérée le plus tard.

Clé métier choisie :

    (icao24, timestamp)

Ordre :

    ingestion_timestamp DESC

row_number = 1
    -> ligne conservée
"""

dedup_window = (
    Window
    .partitionBy(
        "icao24",
        "timestamp",
    )
    .orderBy(
        col("ingestion_timestamp").desc()
    )
)


silver_df = (
    valid_df

    .withColumn(
        "row_num",
        row_number().over(
            dedup_window
        )
    )

    .filter(
        col("row_num") == 1
    )

    .drop(
        "row_num"
    )
)


# ============================================================
# 7. RESULTAT SILVER
# ============================================================

print("\n=== SILVER APRES DEDUP ===")

silver_df.orderBy(
    "timestamp"
).show(
    truncate=False
)


print(
    "Nombre de lignes Bronze :",
    bronze_df.count(),
)

print(
    "Nombre de lignes valides avant dedup :",
    valid_df.count(),
)

print(
    "Nombre de lignes Silver :",
    silver_df.count(),
)


# ============================================================
# 8. SCHEMA SILVER
# ============================================================

print("\n=== SCHEMA SILVER ===")

silver_df.printSchema()


# ============================================================
# 9. ECRITURE DELTA SILVER
# ============================================================

"""
Nous écrivons maintenant une nouvelle table Delta :

    Bronze
        ↓
    transformations
        ↓
    Silver

Le mode overwrite est acceptable ici pour le TP :
nous reconstruisons entièrement la petite table Silver.

Plus tard, dans une architecture incrémentale,
nous utiliserions plutôt MERGE/UPSERT.
"""

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .save(str(SILVER_PATH))
)


print(
    "\nTable Silver créée :",
    SILVER_PATH
)


# ============================================================
# 10. VERIFICATION
# ============================================================

silver_read_df = (
    spark.read
    .format("delta")
    .load(str(SILVER_PATH))
)


print(
    "Nombre de lignes relues depuis Silver :",
    silver_read_df.count()
)


spark.stop()