# ============================================================
# IMPORT
# ============================================================

# On importe le client Python officiel permettant de communiquer
# avec le service BigQuery de Google Cloud.
#
# Au module précédent, nous avions utilisé :
#     from google.cloud import storage
#
# pour communiquer avec GCS.
#
# Ici :
#     bigquery
#
# va nous permettre de créer et piloter un Load Job BigQuery.
from google.cloud import bigquery


# ============================================================
# CONFIGURATION DU PROJET BIGQUERY
# ============================================================

# Identifiant de notre projet Google Cloud.
PROJECT_ID = "toulouse-aviation-data"

# Dataset BigQuery dans lequel se trouve notre table.
DATASET_ID = "aviation_raw"

# Table BigQuery qui contient les positions des avions.
TABLE_ID = "aircraft_positions"


# ============================================================
# SOURCE GCS
# ============================================================

# URI complète du fichier RAW stocké dans Google Cloud Storage.
#
# Rappel :
#
# gs://
#   → indique qu'il s'agit de Google Cloud Storage
#
# toulouse-aviation-data-raw
#   → nom du bucket
#
# raw/streaming/...
#   → chemin logique de l'objet dans le bucket
#
# Notre fichier est du JSONL :
# une position JSON = une ligne physique.
GCS_URI = (
    "gs://toulouse-aviation-data-raw/"
    "raw/streaming/aircraft_positions/"
    "year=2026/month=09/day=01/"
    "aircraft_positions_sample.jsonl"
)


# ============================================================
# CRÉATION DU CLIENT BIGQUERY
# ============================================================

# On crée un client permettant à Python de communiquer avec BigQuery.
#
# IMPORTANT :
# On ne met ici ni mot de passe, ni clé JSON.
#
# La bibliothèque Google va rechercher automatiquement
# les Application Default Credentials (ADC).
#
# Nous configurerons ensuite ADC pour que Python utilise :
#
# aviation-bigquery-loader
#
# via l'impersonation.
#
# ADC = authentification / identité utilisée par l'application.
# IAM = ce que cette identité est autorisée à faire.
client = bigquery.Client(project=PROJECT_ID)


# ============================================================
# IDENTIFIANT COMPLET DE LA TABLE
# ============================================================

# BigQuery identifie complètement une table sous la forme :
#
# projet.dataset.table
#
# Ici cela donnera :
#
# toulouse-aviation-data.aviation_raw.aircraft_positions
table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


# ============================================================
# CONFIGURATION DU LOAD JOB
# ============================================================

# Un Load Job est une opération BigQuery permettant de charger
# des données depuis une source (ici GCS) vers une table BigQuery.
#
# On configure ici le comportement du chargement.
job_config = bigquery.LoadJobConfig(

    # Notre fichier est du JSONL / NDJSON.
    #
    # Rappel :
    # chaque objet JSON doit être sur UNE ligne.
    #
    # {"icao24":"39abcd", ...}
    # {"icao24":"4ca123", ...}
    #
    # C'est justement la différence avec notre premier fichier
    # JSON pretty-print qui avait provoqué une erreur BigQuery.
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,

    # WRITE_APPEND signifie :
    #
    # données déjà présentes
    #          +
    # nouvelles données
    #          =
    # table finale
    #
    # On AJOUTE donc les nouvelles lignes.
    #
    # On n'utilise PAS WRITE_TRUNCATE car celui-ci remplacerait
    # le contenu existant de la table.
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
)


# ============================================================
# LANCEMENT DU LOAD JOB
# ============================================================

# On demande à BigQuery :
#
# 1. de lire le fichier situé dans GCS
# 2. de charger son contenu
# 3. dans notre table BigQuery
# 4. selon la configuration définie ci-dessus
#
# À ce moment-là, BigQuery crée un job asynchrone.
load_job = client.load_table_from_uri(
    GCS_URI,               # fichier source dans GCS
    table_id,              # table destination dans BigQuery
    job_config=job_config  # configuration du chargement
)

print("Chargement BigQuery lancé...")


# ============================================================
# ATTENTE DE LA FIN DU JOB
# ============================================================

# Un Load Job BigQuery est asynchrone :
#
# Python lance le job
#       ↓
# BigQuery travaille côté Google Cloud
#       ↓
# le programme pourrait continuer immédiatement
#
# result() demande donc à Python d'attendre que le job
# soit terminé avant de continuer.
#
# Si le job échoue, une exception sera également remontée ici.
load_job.result()


# ============================================================
# FIN
# ============================================================

print("Chargement terminé.")