from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    min,
    max,
    substring,
    broadcast,
    row_number
)
from pyspark.sql.window import Window


# ============================================================
# 1. Création de la session Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("ToulouseAviationDataPlatform")
    .master("local[2]")
    .getOrCreate()
)

spark.conf.set("spark.sql.shuffle.partitions", "2")

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# 2. Données de positions
#
# On ajoute :
# - timestamp : date/heure de la position
# - ingestion_timestamp : date/heure d'arrivée dans notre pipeline
#
# Cela va nous permettre de gérer les doublons.
# ============================================================

aircraft_positions = [
    (
        "39abcd",
        "AFR123",
        43.6047,
        1.4442,
        10000.0,
        "2026-09-03 13:00:00",
        "2026-09-03 13:00:05"
    ),

    # Même avion + même timestamp métier
    # mais valeur reçue plus tard
    (
        "39abcd",
        "AFR123",
        43.6047,
        1.4442,
        10100.0,
        "2026-09-03 13:00:00",
        "2026-09-03 13:00:08"
    ),

    (
        "39abcd",
        "AFR123",
        43.7000,
        1.5000,
        10500.0,
        "2026-09-03 13:01:00",
        "2026-09-03 13:01:05"
    ),

    (
        "4ca123",
        "RYR456",
        44.1000,
        2.0000,
        9000.0,
        "2026-09-03 13:02:00",
        "2026-09-03 13:02:05"
    ),

    # Données invalides
    (
        "bad001",
        "TEST01",
        43.6000,
        1.4000,
        -500.0,
        "2026-09-03 13:03:00",
        "2026-09-03 13:03:05"
    ),

    (
        "bad002",
        "TEST02",
        43.6000,
        1.4000,
        None,
        "2026-09-03 13:04:00",
        "2026-09-03 13:04:05"
    ),
]


# ============================================================
# 3. Création du DataFrame
# ============================================================

df = spark.createDataFrame(
    aircraft_positions,
    [
        "icao24",
        "callsign",
        "latitude",
        "longitude",
        "altitude",
        "timestamp",
        "ingestion_timestamp"
    ]
)


# ============================================================
# 4. Référentiel compagnies
# ============================================================

airlines = [
    ("AFR", "Air France", "France"),
    ("RYR", "Ryanair", "Ireland"),
    ("EZY", "easyJet", "United Kingdom"),
]

df_airlines = spark.createDataFrame(
    airlines,
    [
        "airline_code",
        "airline_name",
        "airline_country"
    ]
)


# ============================================================
# 5. Affichage RAW
# ============================================================

print("\n=== DONNEES RAW ===")

df.show(truncate=False)


# ============================================================
# 6. Filtrage qualité
# ============================================================

df_valid = df.filter(
    (col("altitude").isNotNull()) &
    (col("altitude") >= 0)
)


print("\n=== DONNEES VALIDES ===")

df_valid.show(truncate=False)


# ============================================================
# 7. Définition de la Window
#
# On considère qu'un événement est identifié par :
#
#     (icao24, timestamp)
#
# Si plusieurs lignes existent pour cette clé,
# on garde celle dont ingestion_timestamp est le plus récent.
# ============================================================

dedup_window = (
    Window
    .partitionBy("icao24", "timestamp")
    .orderBy(col("ingestion_timestamp").desc())
)


# ============================================================
# 8. Numérotation des doublons
# ============================================================

df_ranked = df_valid.withColumn(
    "row_number",
    row_number().over(dedup_window)
)


print("\n=== DONNEES AVEC NUMERO DE LIGNE ===")

df_ranked.select(
    "icao24",
    "timestamp",
    "altitude",
    "ingestion_timestamp",
    "row_number"
).show(truncate=False)


# ============================================================
# 9. Déduplication
#
# On conserve uniquement row_number = 1
# ============================================================

df_dedup = (
    df_ranked
    .filter(col("row_number") == 1)
    .drop("row_number")
)


print("\n=== DONNEES APRES DEDUPLICATION ===")

df_dedup.select(
    "icao24",
    "callsign",
    "timestamp",
    "altitude",
    "ingestion_timestamp"
).show(truncate=False)


# ============================================================
# 10. Enrichissement altitude
# ============================================================

df_enriched = df_dedup.withColumn(
    "altitude_feet",
    col("altitude") * 3.28084
)


# ============================================================
# 11. Extraction du code compagnie
# ============================================================

df_with_airline_code = df_enriched.withColumn(
    "airline_code",
    substring(col("callsign"), 1, 3)
)


# ============================================================
# 12. LEFT JOIN + BROADCAST
# ============================================================

df_joined = df_with_airline_code.join(
    broadcast(df_airlines),
    on="airline_code",
    how="left"
)


print("\n=== POSITIONS FINALES ENRICHIES ===")

df_joined.select(
    "icao24",
    "callsign",
    "timestamp",
    "altitude",
    "altitude_feet",
    "airline_name",
    "airline_country"
).show(truncate=False)


# ============================================================
# 13. Statistiques par avion
# ============================================================

df_stats = (
    df_dedup
    .groupBy("icao24")
    .agg(
        count("*").alias("position_count"),
        avg("altitude").alias("avg_altitude"),
        min("altitude").alias("min_altitude"),
        max("altitude").alias("max_altitude")
    )
)


print("\n=== STATISTIQUES PAR AVION ===")

df_stats.show()


# ============================================================
# 14. Plan d'exécution
# ============================================================

print("\n=== PLAN D'EXECUTION DEDUPLICATION ===")

df_dedup.explain()


# ============================================================
# 15. Arrêt propre
# ============================================================

spark.stop()