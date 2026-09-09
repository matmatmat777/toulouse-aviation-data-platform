# Importe la bibliothèque cliente Google Cloud Storage.
# Elle permet à Python de communiquer avec GCS :
# créer des objets, les lire, les supprimer, etc.
# Les actions réellement autorisées dépendront ensuite des droits IAM.
from google.cloud import storage

# Module standard Python permettant de manipuler du JSON.
import json


# -------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------

# Nom du bucket GCS dans lequel nous voulons écrire.
# Un bucket est le conteneur principal de nos objets dans Cloud Storage.
BUCKET_NAME = "toulouse-aviation-data-raw"


# Chemin logique de l'objet dans le bucket.
#
# Attention :
# GCS n'utilise pas réellement des dossiers comme Windows.
# Tout ceci fait partie du NOM de l'objet.
#
# Cette organisation permet de partitionner logiquement les données
# par année / mois / jour.
OBJECT_PATH = (
    "raw/streaming/aircraft_positions/"
    "year=2026/month=09/day=01/"
    "positions_python_test_03.json"
)


# -------------------------------------------------------------------
# DONNÉES À ENVOYER
# -------------------------------------------------------------------

# Pour ce premier test, nous simulons la position d'un avion.
#
# Plus tard, ces données ne seront plus écrites manuellement :
# elles proviendront d'une API puis passeront par notre pipeline.
data = {
    "icao24": "39abcd",
    "callsign": "AFR123",
    "latitude": 43.6047,
    "longitude": 1.4442,
    "altitude": 11000,
    "timestamp": "2026-09-01T16:00:00Z"
}


# -------------------------------------------------------------------
# CONNEXION À GOOGLE CLOUD
# -------------------------------------------------------------------

# Création du client Google Cloud Storage.
#
# Nous ne fournissons ici :
# - aucun login
# - aucun mot de passe
# - aucune clé JSON
#
# La bibliothèque Google cherche automatiquement des
# Application Default Credentials (ADC).
#
# Pour le moment, nos ADC correspondent à notre utilisateur local.
client = storage.Client()


# -------------------------------------------------------------------
# SÉLECTION DU BUCKET
# -------------------------------------------------------------------

# On crée une référence Python vers notre bucket.
#
# Cette instruction ne télécharge pas les données du bucket.
# Elle indique simplement au client GCS sur quel bucket
# nous souhaitons travailler.
bucket = client.bucket(BUCKET_NAME)


# -------------------------------------------------------------------
# SÉLECTION DE L'OBJET
# -------------------------------------------------------------------

# Dans Google Cloud Storage, un fichier stocké est appelé un "objet".
#
# Dans la bibliothèque Python, cet objet est représenté par un Blob.
#
# Ici nous préparons donc la référence vers :
#
# raw/streaming/aircraft_positions/
# year=2026/month=09/day=01/
# positions_python_test.json
blob = bucket.blob(OBJECT_PATH)


# -------------------------------------------------------------------
# ÉCRITURE DANS GCS
# -------------------------------------------------------------------

# data est actuellement un dictionnaire Python.
#
# json.dumps(data) le transforme en chaîne JSON.
#
# Exemple :
#
# dictionnaire Python
# {"callsign": "AFR123"}
#
#        ↓ json.dumps()
#
# texte JSON
# '{"callsign": "AFR123"}'
#
# upload_from_string() envoie ensuite ce contenu dans GCS.
blob.upload_from_string(
    json.dumps(data),
    content_type="application/json"
)


# -------------------------------------------------------------------
# CONFIRMATION
# -------------------------------------------------------------------

# Si le programme arrive jusqu'ici sans exception,
# l'upload a réussi.
#
# gs:// est la notation utilisée pour désigner une ressource GCS.
print(f"Objet créé : gs://{BUCKET_NAME}/{OBJECT_PATH}")

# -------------------------------------------------------------------
# TEST DE LECTURE
# -------------------------------------------------------------------

# On essaie maintenant de lire un objet déjà présent dans le bucket.
# Notre Service Account possède uniquement le rôle
# "Storage Object Creator".
#
# Il devrait donc pouvoir créer un objet,
# mais PAS lire son contenu.

blob_to_read = bucket.blob(
    "raw/streaming/aircraft_positions/"
    "year=2026/month=09/day=01/"
    "positions_python_test_02.json"
)

print("Tentative de lecture de l'objet...")

content = blob_to_read.download_as_text()

print(content)