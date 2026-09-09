from google.cloud import storage
import json
from datetime import datetime, timezone


# -------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------

BUCKET_NAME = "toulouse-aviation-data-raw"


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

    # On récupère l'heure actuelle en UTC.
    #
    # UTC est préférable dans un pipeline Data :
    # les avions et les systèmes peuvent fonctionner
    # dans différents fuseaux horaires.
    now = datetime.now(timezone.utc)

    # Construction dynamique du chemin GCS.
    #
    # Exemple :
    #
    # raw/streaming/aircraft_positions/
    # year=2026/month=09/day=01/
    # positions_20260901_160530.json
    #
    # On ne code donc plus manuellement :
    # year=2026/month=09/day=01
    object_path = (
        "raw/streaming/aircraft_positions/"
        f"year={now:%Y}/"
        f"month={now:%m}/"
        f"day={now:%d}/"
        f"positions_{now:%Y%m%d_%H%M%S_%f}.json"
    )

    # Création d'une référence vers le futur objet GCS.
    blob = bucket.blob(object_path)

    # Transformation :
    #
    # dictionnaire Python
    #       ↓
    # chaîne JSON
    #       ↓
    # upload dans GCS
    blob.upload_from_string(
        json.dumps(data),
        content_type="application/json"
    )

    # Construction de l'URI complète.
    gcs_uri = f"gs://{BUCKET_NAME}/{object_path}"

    return gcs_uri


# -------------------------------------------------------------------
# TEST LOCAL
# -------------------------------------------------------------------

if __name__ == "__main__":

    # Événement fictif représentant une position d'avion.
    test_position = {
        "icao24": "39abcd",
        "callsign": "AFR123",
        "latitude": 43.6047,
        "longitude": 1.4442,
        "altitude": 11000,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    uri = upload_aircraft_position(test_position)

    print(f"Objet RAW créé : {uri}")