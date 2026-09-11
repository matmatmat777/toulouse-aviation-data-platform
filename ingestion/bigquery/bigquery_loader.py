# ============================================================
# IMPORTS
# ============================================================

import os

from google.cloud import bigquery


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ID = os.getenv(
    "GCP_PROJECT_ID",
    "toulouse-aviation-data",
)

DATASET_ID = os.getenv(
    "BIGQUERY_DATASET_ID",
    "aviation_raw",
)

TABLE_ID = os.getenv(
    "BIGQUERY_TABLE_ID",
    "aircraft_positions",
)

RAW_BUCKET = os.getenv(
    "AVIATION_RAW_BUCKET",
    "toulouse-aviation-data-raw",
)

GCS_OBJECT_PATH = os.getenv(
    "BIGQUERY_SOURCE_OBJECT",
    (
        "raw/streaming/aircraft_positions/"
        "year=2026/month=09/day=01/"
        "aircraft_positions_sample.jsonl"
    ),
)

GCS_URI = (
    f"gs://{RAW_BUCKET}/"
    f"{GCS_OBJECT_PATH}"
)


# ============================================================
# CRÉATION DU CLIENT BIGQUERY
# ============================================================

client = bigquery.Client(
    project=PROJECT_ID
)


# ============================================================
# IDENTIFIANT COMPLET DE LA TABLE
# ============================================================

table_id = (
    f"{PROJECT_ID}."
    f"{DATASET_ID}."
    f"{TABLE_ID}"
)


# ============================================================
# CONFIGURATION DU LOAD JOB
# ============================================================

job_config = bigquery.LoadJobConfig(

    source_format=(
        bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    ),

    write_disposition=(
        bigquery.WriteDisposition.WRITE_APPEND
    ),
)


# ============================================================
# LANCEMENT DU LOAD JOB
# ============================================================

print(
    "========================================"
)
print(
    "BIGQUERY LOAD JOB"
)
print(
    "========================================"
)

print(
    f"Projet        : {PROJECT_ID}"
)
print(
    f"Dataset       : {DATASET_ID}"
)
print(
    f"Table         : {TABLE_ID}"
)
print(
    f"Source GCS    : {GCS_URI}"
)
print(
    f"Destination   : {table_id}"
)

load_job = client.load_table_from_uri(
    GCS_URI,
    table_id,
    job_config=job_config,
)

print(
    "Chargement BigQuery lancé..."
)


# ============================================================
# ATTENTE DE LA FIN DU JOB
# ============================================================

load_job.result()


# ============================================================
# FIN
# ============================================================

print(
    "Chargement terminé."
)