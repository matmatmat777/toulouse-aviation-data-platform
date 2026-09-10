"""
TP DELTA LAKE - SCHEMA ENFORCEMENT
==================================

Objectif :
Comprendre comment Delta Lake protège le schéma d'une table.

Notre table Delta possède actuellement un schéma précis :

    altitude
    callsign
    icao24
    ingestion_timestamp
    latitude
    longitude
    timestamp

Nous allons essayer d'écrire une ligne contenant une colonne
supplémentaire :

    aircraft_type

Cette colonne n'existe pas dans la table Delta.

Sans autorisation explicite de faire évoluer le schéma,
Delta doit refuser cette écriture.

C'est le SCHEMA ENFORCEMENT.
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
# 2. CREATION DE LA SESSION SPARK + DELTA
# ============================================================

builder = (
    SparkSession.builder
    .appName("aviation-delta-schema-demo")
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
# 3. LECTURE DU SCHEMA ACTUEL
# ============================================================

current_df = (
    spark.read
    .format("delta")
    .load(str(DELTA_PATH))
)

print("\n=== SCHEMA ACTUEL DE LA TABLE DELTA ===")

current_df.printSchema()


# ============================================================
# 4. CREATION D'UNE DONNEE AVEC UN SCHEMA DIFFERENT
# ============================================================

"""
Cette nouvelle ligne contient une colonne supplémentaire :

    aircraft_type = "A320"

Cette colonne n'existe PAS encore dans notre table Delta.

Le DataFrame source aura donc un schéma différent du
schéma de la table cible.
"""

new_data = [
    {
        "altitude": "12500.0",
        "callsign": "AFR777",
        "icao24": "new777",
        "ingestion_timestamp": "2026-09-03T13:10:05",
        "latitude": 43.90,
        "longitude": 1.80,
        "timestamp": "2026-09-03T13:10:00",

        # Nouvelle colonne volontairement ajoutée.
        "aircraft_type": "A320",
    }
]

new_df = spark.createDataFrame(new_data)


print("\n=== SCHEMA DU NOUVEAU DATAFRAME ===")

new_df.printSchema()


# ============================================================
# 5. TENTATIVE D'ECRITURE
# ============================================================

"""
Nous faisons volontairement un append classique.

IMPORTANT :

Nous n'utilisons PAS :

    .option("mergeSchema", "true")

Delta n'a donc aucune autorisation pour modifier
automatiquement le schéma de la table.

Résultat attendu :

    ECHEC

Delta devrait signaler une incompatibilité de schéma.
"""

print("\n=== TENTATIVE D'ECRITURE ===")

try:

    (
        new_df.write
        .format("delta")
        .mode("append")
        .save(str(DELTA_PATH))
    )

    # Si nous arrivons ici, l'écriture a été acceptée,
    # ce qui n'est PAS le résultat attendu dans ce TP.
    print(
        "ERREUR PEDAGOGIQUE : "
        "l'écriture a été acceptée alors qu'on attendait un refus."
    )

except Exception as error:

    # Ici, au contraire, l'échec est une réussite pédagogique :
    # Delta a protégé le schéma de la table.
    print("\n=== ECRITURE REFUSEE PAR DELTA ===")

    print(type(error).__name__)

    # Le message complet Spark/Delta peut être très long.
    # On n'en affiche que le début.
    print(str(error)[:1500])

    print(
        "\nRESULTAT : Schema Enforcement fonctionne."
    )


# ============================================================
# 6. VERIFICATION
# ============================================================

"""
L'écriture ayant échoué, la table ne doit pas avoir changé.

Nous avions 11 lignes après le MERGE.

Nous devons donc toujours avoir :

    11 lignes

et la colonne aircraft_type ne doit toujours pas faire
partie du schéma.
"""

after_df = (
    spark.read
    .format("delta")
    .load(str(DELTA_PATH))
)

print("\n=== TABLE APRES LA TENTATIVE ===")

print(
    "Nombre de lignes :",
    after_df.count(),
)

after_df.printSchema()


# ============================================================
# 7. FIN
# ============================================================

spark.stop()