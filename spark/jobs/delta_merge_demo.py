"""
TP DELTA LAKE - MERGE / UPSERT
==============================

Objectif pédagogique :
- comprendre le fonctionnement d'un MERGE Delta Lake ;
- mettre à jour une ligne existante ;
- insérer une ligne qui n'existe pas ;
- comprendre la différence entre APPEND et MERGE.

Cas étudié :

La table Delta contient déjà l'avion :

    icao24 = 400abc
    timestamp = 2026-09-03T13:06:00
    altitude = 11000.0

Un nouveau lot arrive avec :

1. le même avion 400abc, mais avec une altitude corrigée à 11500.0 ;
2. un nouvel avion new999.

Avec APPEND :
    → les deux lignes seraient simplement ajoutées ;
    → 400abc pourrait donc devenir un doublon logique.

Avec MERGE :
    → si la ligne existe : UPDATE ;
    → si elle n'existe pas : INSERT.

C'est ce qu'on appelle un UPSERT :
    UPDATE + INSERT.
"""

from pathlib import Path

# configure_spark_with_delta_pip permet de démarrer Spark
# avec les dépendances Delta Lake installées via le package delta-spark.
from delta import configure_spark_with_delta_pip

# DeltaTable fournit l'API spécifique aux tables Delta.
# C'est notamment grâce à elle que nous pouvons utiliser MERGE.
from delta.tables import DeltaTable

from pyspark.sql import SparkSession


# ============================================================
# 1. CHEMINS DU PROJET
# ============================================================

# __file__ correspond au fichier actuel :
#
# spark/jobs/delta_merge_demo.py
#
# parents[2] nous permet donc de remonter à la racine :
#
# toulouse-aviation-data-platform/
PROJECT_DIR = Path(__file__).resolve().parents[2]


# Notre table Bronze Delta créée lors du TP précédent.
#
# Structure physique :
#
# data/
# └── delta/
#     └── bronze/
#         └── aircraft_positions/
#             ├── _delta_log/
#             └── part-....parquet
DELTA_PATH = (
    PROJECT_DIR
    / "data"
    / "delta"
    / "bronze"
    / "aircraft_positions"
)


# ============================================================
# 2. CREATION DE LA SPARK SESSION AVEC DELTA
# ============================================================

builder = (
    SparkSession.builder

    # Nom visible du job Spark.
    .appName("aviation-delta-merge-demo")

    # local[2] signifie :
    # exécution locale avec 2 threads disponibles.
    .master("local[2]")

    # Active les extensions SQL nécessaires à Delta Lake.
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension",
    )

    # Remplace le catalogue Spark standard par un catalogue
    # capable de gérer les fonctionnalités Delta.
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)


# configure_spark_with_delta_pip complète la configuration
# avec les dépendances Delta nécessaires côté JVM.
spark = configure_spark_with_delta_pip(
    builder
).getOrCreate()


# On réduit la quantité de logs Spark affichés dans le terminal
# pour rendre notre TP plus lisible.
spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 3. SIMULATION D'UN NOUVEAU LOT DE DONNEES
# ============================================================

source_data = [

    # --------------------------------------------------------
    # CAS 1 : avion déjà présent dans la table
    # --------------------------------------------------------
    #
    # 400abc existe déjà pour :
    #
    # timestamp = 2026-09-03T13:06:00
    #
    # Mais nous recevons maintenant une donnée plus récente
    # indiquant notamment :
    #
    # altitude = 11500.0
    #
    # Le MERGE devra donc effectuer un UPDATE.
    {
        "altitude": "11500.0",
        "callsign": "EZY789",
        "icao24": "400abc",
        "ingestion_timestamp": "2026-09-03T13:09:05",
        "latitude": 43.66,
        "longitude": 1.51,
        "timestamp": "2026-09-03T13:06:00",
    },


    # --------------------------------------------------------
    # CAS 2 : nouvel avion
    # --------------------------------------------------------
    #
    # new999 n'existe pas encore dans notre table Delta.
    #
    # Le MERGE devra donc effectuer un INSERT.
    {
        "altitude": "9800.0",
        "callsign": "NEW999",
        "icao24": "new999",
        "ingestion_timestamp": "2026-09-03T13:09:10",
        "latitude": 43.80,
        "longitude": 1.70,
        "timestamp": "2026-09-03T13:09:00",
    },
]


# Transformation de notre liste Python en DataFrame Spark.
#
# source_df représente donc le nouveau lot entrant.
source_df = spark.createDataFrame(source_data)


print("\n=== NOUVEAU LOT SOURCE ===")

source_df.show(
    truncate=False
)


# ============================================================
# 4. OUVERTURE DE LA TABLE DELTA EXISTANTE
# ============================================================

# Attention :
#
# spark.read.format("delta").load(...)
#
# nous donnerait un DataFrame.
#
# Ici nous utilisons DeltaTable.forPath(...)
# car nous voulons effectuer une opération Delta spécifique :
#
# MERGE.
delta_table = DeltaTable.forPath(
    spark,
    str(DELTA_PATH),
)


# ============================================================
# 5. ETAT DE LA TABLE AVANT LE MERGE
# ============================================================

print("\n=== AVANT MERGE ===")


# On n'affiche que les deux avions qui nous intéressent.
#
# Avant le MERGE, nous devrions avoir :
#
# 400abc → présent
# new999 → absent
(
    delta_table
    .toDF()
    .filter("icao24 IN ('400abc', 'new999')")
    .show(truncate=False)
)


print(
    "Nombre total de lignes AVANT MERGE :",
    delta_table.toDF().count(),
)


# ============================================================
# 6. MERGE DELTA
# ============================================================

"""
C'est le cœur du TP.

Nous avons :

TARGET
------
La table Delta existante.

SOURCE
------
Le nouveau lot source_df.

Nous allons comparer les lignes avec la condition :

    target.icao24 = source.icao24
    AND
    target.timestamp = source.timestamp

Autrement dit, dans notre exemple, l'identité logique
d'un événement est :

    (icao24, timestamp)

Exemple :

    400abc + 2026-09-03T13:06:00

Si cette combinaison existe déjà :
    → MATCHED
    → UPDATE

Si cette combinaison n'existe pas :
    → NOT MATCHED
    → INSERT
"""


(
    delta_table.alias("target")

    # --------------------------------------------------------
    # CONDITION DE CORRESPONDANCE
    # --------------------------------------------------------
    .merge(
        source_df.alias("source"),
        """
        target.icao24 = source.icao24
        AND target.timestamp = source.timestamp
        """
    )

    # --------------------------------------------------------
    # MATCHED → UPDATE
    # --------------------------------------------------------
    #
    # Une ligne correspondante existe déjà.
    #
    # Exemple :
    #
    # target :
    # 400abc | 13:06 | altitude 11000
    #
    # source :
    # 400abc | 13:06 | altitude 11500
    #
    # Résultat :
    #
    # 400abc | 13:06 | altitude 11500
    #
    .whenMatchedUpdateAll()

    # --------------------------------------------------------
    # NOT MATCHED → INSERT
    # --------------------------------------------------------
    #
    # Aucune ligne correspondante n'existe.
    #
    # Exemple :
    #
    # new999
    #
    # sera ajouté à la table.
    .whenNotMatchedInsertAll()

    # Exécution réelle de la transaction Delta.
    .execute()
)


# ============================================================
# 7. ETAT DE LA TABLE APRES LE MERGE
# ============================================================

print("\n=== APRES MERGE ===")


(
    delta_table
    .toDF()
    .filter("icao24 IN ('400abc', 'new999')")
    .show(truncate=False)
)


print(
    "Nombre total de lignes APRES MERGE :",
    delta_table.toDF().count(),
)


# ============================================================
# 8. FIN DU JOB
# ============================================================

spark.stop()