import json
import os
from datetime import datetime, timezone

from google.cloud import storage


# -------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------

BUCKET_NAME = os.getenv(
    "AVIATION_RAW_BUCKET",
    "toulouse-aviation-data-raw",
)


# -------------------------------------------------------------------
# CLIENT GCS
# -------------------------------------------------------------------

# Création du client Google Cloud Storage.
#
# Aucun mot de passe ou fichier de clé n'est présent dans le code.
# Google Cloud utilise automatiquement les credentials ADC.
#
# Dans notre environnement actuel, ADC utilise l'impersonation
# du Service Account aviation-gcs-writer.
client = storage.Client()

# Référence vers notre bucket RAW.
bucket = client.bucket(BUCKET_NAME)


# -------------------------------------------------------------------
# FONCTION D'ÉCRITURE
# -------------------------------------------------------------------

def upload_aircraft_position(data: dict) -> str:
    """
    Enregistre une position d'avion au format JSON
    dans la zone RAW de Google Cloud Storage.

    La fonction retourne l'URI GCS de l'objet créé.
    """

    now = datetime.now(timezone.utc)

    object_path = (
        "raw/streaming/aircraft_positions/"
        f"year={now:%Y}/"
        f"month={now:%m}/"
        f"day={now:%d}/"
        f"positions_{now:%Y%m%d_%H%M%S_%f}.json"
    )

    blob = bucket.blob(object_path)

    blob.upload_from_string(
        json.dumps(data),
        content_type="application/json",
    )

    gcs_uri = f"gs://{BUCKET_NAME}/{object_path}"

    return gcs_uri


# -------------------------------------------------------------------
# TEST LOCAL
# -------------------------------------------------------------------

if __name__ == "__main__":

    test_position = {
        "icao24": "39abcd",
        "callsign": "AFR123",
        "latitude": 43.6047,
        "longitude": 1.4442,
        "altitude": 11000,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    uri = upload_aircraft_position(
        test_position
    )

    print(
        f"Objet RAW créé : {uri}"
    )