"""
TP DELTA LAKE - SCHEMA EVOLUTION
================================

Objectif :
Comprendre comment faire évoluer volontairement le schéma
d'une table Delta.

Dans le TP précédent :

    append + nouvelle colonne
            ↓
    Schema Enforcement
            ↓
          REFUS

Cette fois nous allons explicitement autoriser Delta à
faire évoluer le schéma grâce à :

    .option("mergeSchema", "true")

Nous ajoutons la colonne :

    aircraft_type

Résultat attendu :

- les 11 anciennes lignes auront aircraft_type = NULL ;
- la nouvelle ligne aura aircraft_type = "A320" ;
- la table contiendra 12 lignes ;
- aircraft_type fera désormais partie du schéma Delta.
"""

from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


# ============================================================
# 1. CHEMIN DE LA TABLE DELTA
# ============================================================

PROJECT_DIR = Path(__file__).resolve().parents[2]

DELTA_PATH = (
    PROJECT_DIR
    / "data"
    / "delta"
    / "bronze"
    / "aircraft_positions"
)


# ============================================================
# 2. SESSION SPARK + DELTA
# ============================================================

builder = (
    SparkSession.builder
    .appName("aviation-delta-schema-evolution-demo")
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
# 3. SCHEMA AVANT EVOLUTION
# ============================================================

before_df = (
    spark.read
    .format("delta")
    .load(str(DELTA_PATH))
)

print("\n=== AVANT SCHEMA EVOLUTION ===")

before_df.printSchema()

print(
    "Nombre de lignes :",
    before_df.count(),
)


# ============================================================
# 4. NOUVELLE DONNEE
# ============================================================

# Cette ligne contient aircraft_type.
#
# Cette colonne n'existe pas encore dans le schéma de
# notre table Delta.

new_data = [
    {
        "altitude": "12500.0",
        "callsign": "AFR777",
        "icao24": "new777",
        "ingestion_timestamp": "2026-09-03T13:10:05",
        "latitude": 43.90,
        "longitude": 1.80,
        "timestamp": "2026-09-03T13:10:00",

        # Nouvelle colonne
        "aircraft_type": "A320",
    }
]

new_df = spark.createDataFrame(new_data)


print("\n=== SCHEMA DU NOUVEAU DATAFRAME ===")

new_df.printSchema()


# ============================================================
# 5. SCHEMA EVOLUTION
# ============================================================

"""
C'est LA ligne importante du TP :

    .option("mergeSchema", "true")

Nous disons explicitement à Delta :

    "Si mon DataFrame contient de nouvelles colonnes
     compatibles, fais évoluer le schéma de la table."

Attention :

Schema Evolution n'est donc pas la même chose que
Schema Enforcement.

Schema Enforcement :
    protège le schéma existant.

Schema Evolution :
    autorise explicitement certaines modifications
    du schéma.
"""

(
    new_df.write
    .format("delta")
    .mode("append")

    # Autorisation explicite de faire évoluer le schéma.
    .option("mergeSchema", "true")

    .save(str(DELTA_PATH))
)


print("\n=== ECRITURE ACCEPTEE ===")


# ============================================================
# 6. RELECTURE DE LA TABLE
# ============================================================

# Nous relisons la table APRÈS l'écriture afin de récupérer
# son nouveau schéma.

after_df = (
    spark.read
    .format("delta")
    .load(str(DELTA_PATH))
)


print("\n=== NOUVEAU SCHEMA DELTA ===")

after_df.printSchema()


# ============================================================
# 7. VERIFICATION DU NOMBRE DE LIGNES
# ============================================================

print(
    "\nNombre total de lignes :",
    after_df.count(),
)


# ============================================================
# 8. VERIFICATION DE LA NOUVELLE LIGNE
# ============================================================

print("\n=== NOUVEL AVION ===")

(
    after_df
    .filter("icao24 = 'new777'")
    .select(
        "icao24",
        "callsign",
        "altitude",
        "aircraft_type",
    )
    .show(truncate=False)
)


# ============================================================
# 9. VERIFICATION DES ANCIENNES LIGNES
# ============================================================

print("\n=== EXEMPLE D'ANCIENNE LIGNE ===")

(
    after_df
    .filter("icao24 = '400abc'")
    .select(
        "icao24",
        "callsign",
        "altitude",
        "aircraft_type",
    )
    .show(truncate=False)
)


# ============================================================
# 10. COMPTAGE DES NULL
# ============================================================

"""
Nous avions 11 lignes avant l'évolution.

Elles ne possédaient pas aircraft_type.

Delta les expose donc maintenant comme :

    aircraft_type = NULL

La nouvelle ligne possède :

    aircraft_type = A320
"""

null_count = (
    after_df
    .filter("aircraft_type IS NULL")
    .count()
)

print(
    "Nombre de lignes avec aircraft_type = NULL :",
    null_count,
)


# ============================================================
# 11. FIN
# ============================================================

spark.stop()