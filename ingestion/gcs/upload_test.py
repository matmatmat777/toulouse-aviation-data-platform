import json
import os

from google.cloud import storage


# -------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------

BUCKET_NAME = os.getenv(
    "AVIATION_RAW_BUCKET",
    "toulouse-aviation-data-raw",
)

OBJECT_PATH = (
    "raw/streaming/aircraft_positions/"
    "year=2026/month=09/day=01/"
    "positions_python_test_03.json"
)


# -------------------------------------------------------------------
# DONNÉES À ENVOYER
# -------------------------------------------------------------------

data = {
    "icao24": "39abcd",
    "callsign": "AFR123",
    "latitude": 43.6047,
    "longitude": 1.4442,
    "altitude": 11000,
    "timestamp": "2026-09-01T16:00:00Z",
}


# -------------------------------------------------------------------
# CONNEXION À GOOGLE CLOUD
# -------------------------------------------------------------------

client = storage.Client()


# -------------------------------------------------------------------
# SÉLECTION DU BUCKET
# -------------------------------------------------------------------

bucket = client.bucket(
    BUCKET_NAME
)


# -------------------------------------------------------------------
# SÉLECTION DE L'OBJET
# -------------------------------------------------------------------

blob = bucket.blob(
    OBJECT_PATH
)


# -------------------------------------------------------------------
# ÉCRITURE DANS GCS
# -------------------------------------------------------------------

blob.upload_from_string(
    json.dumps(data),
    content_type="application/json",
)


# -------------------------------------------------------------------
# CONFIRMATION
# -------------------------------------------------------------------

print(
    f"Objet créé : "
    f"gs://{BUCKET_NAME}/{OBJECT_PATH}"
)


# -------------------------------------------------------------------
# TEST DE LECTURE
# -------------------------------------------------------------------

blob_to_read = bucket.blob(
    "raw/streaming/aircraft_positions/"
    "year=2026/month=09/day=01/"
    "positions_python_test_02.json"
)

print(
    "Tentative de lecture de l'objet..."
)

content = (
    blob_to_read.download_as_text()
)

print(
    content
)