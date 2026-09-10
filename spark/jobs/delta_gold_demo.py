"""
TP DELTA LAKE - SILVER -> GOLD
==============================

Objectif :
Construire une table Gold orientée métier.

Nous partons de la table Silver propre et typée.

Nous allons :
1. extraire le code compagnie depuis callsign ;
2. joindre avec le référentiel airlines.csv ;
3. agréger les données ;
4. écrire le résultat en Delta Gold.

Exemples :

AFR123 -> AFR -> Air France
RYR456 -> RYR -> Ryanair
EZY789 -> EZY -> easyJet
"""

from pathlib import Path

from delta import configure_spark_with_delta_pip

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# 1. CHEMINS
# ============================================================

PROJECT_DIR = Path(__file__).resolve().parents[2]

SILVER_PATH = (
    PROJECT_DIR
    / "data"
    / "delta"
    / "silver"
    / "aircraft_positions"
)

AIRLINES_PATH = (
    PROJECT_DIR
    / "data"
    / "reference"
    / "airlines.csv"
)

GOLD_PATH = (
    PROJECT_DIR
    / "data"
    / "delta"
    / "gold"
    / "airline_statistics"
)


# ============================================================
# 2. SESSION SPARK + DELTA
# ============================================================

builder = (
    SparkSession.builder
    .appName("aviation-delta-gold-demo")
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
# 3. LECTURE SILVER
# ============================================================

silver_df = (
    spark.read
    .format("delta")
    .load(str(SILVER_PATH))
)

print("\n=== SILVER ===")

silver_df.orderBy(
    "timestamp"
).show(
    truncate=False
)

print(
    "Nombre de lignes Silver :",
    silver_df.count()
)


# ============================================================
# 4. EXTRACTION DU CODE COMPAGNIE
# ============================================================

"""
Le callsign contient généralement un préfixe compagnie.

Exemples :

AFR123 -> AFR
RYR456 -> RYR
DLH321 -> DLH

substring commence à 1 dans Spark SQL.
"""

silver_with_airline_df = (
    silver_df
    .withColumn(
        "airline_code",
        F.substring(
            F.col("callsign"),
            1,
            3,
        )
    )
)


print("\n=== SILVER AVEC AIRLINE_CODE ===")

silver_with_airline_df.select(
    "icao24",
    "callsign",
    "airline_code",
    "altitude",
    "timestamp",
).show(
    truncate=False
)


# ============================================================
# 5. LECTURE DU REFERENTIEL COMPAGNIES
# ============================================================

airlines_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(str(AIRLINES_PATH))
)


print("\n=== REFERENTIEL AIRLINES ===")

airlines_df.show(
    truncate=False
)


# ============================================================
# 6. JOINTURE
# ============================================================

"""
On enrichit la Silver avec le nom complet de la compagnie.

LEFT JOIN :
on conserve toutes les positions Silver,
même si le code compagnie n'existe pas dans le référentiel.
"""

enriched_df = (
    silver_with_airline_df.alias("positions")
    .join(
        airlines_df.alias("airlines"),
        F.col("positions.airline_code")
        == F.col("airlines.airline_code"),
        "left",
    )
    .select(
        F.col("positions.*"),
        F.col("airlines.airline_name"),
    )
)


print("\n=== DONNEES ENRICHIES ===")

enriched_df.select(
    "callsign",
    "airline_code",
    "airline_name",
    "altitude",
).show(
    truncate=False
)


# ============================================================
# 7. AGREGATION GOLD
# ============================================================

"""
Nous construisons maintenant un indicateur métier :

pour chaque compagnie :
- nombre de positions observées ;
- altitude moyenne ;
- altitude minimum ;
- altitude maximum.
"""

gold_df = (
    enriched_df

    .groupBy(
        "airline_code",
        "airline_name",
    )

    .agg(
        F.count("*").alias(
            "position_count"
        ),

        F.round(
            F.avg("altitude"),
            2,
        ).alias(
            "average_altitude"
        ),

        F.min("altitude").alias(
            "min_altitude"
        ),

        F.max("altitude").alias(
            "max_altitude"
        ),
    )

    .orderBy(
        F.col("position_count").desc()
    )
)


print("\n=== GOLD : STATISTIQUES PAR COMPAGNIE ===")

gold_df.show(
    truncate=False
)


# ============================================================
# 8. ECRITURE DELTA GOLD
# ============================================================

(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .save(str(GOLD_PATH))
)


print(
    "\nTable Gold créée :",
    GOLD_PATH
)


# ============================================================
# 9. VERIFICATION
# ============================================================

gold_read_df = (
    spark.read
    .format("delta")
    .load(str(GOLD_PATH))
)


print("\n=== GOLD RELUE DEPUIS DELTA ===")

gold_read_df.show(
    truncate=False
)

print(
    "Nombre de lignes Gold :",
    gold_read_df.count()
)


spark.stop()
