# ============================================================
# KAFKA -> GCS RAW CONSUMER
# TOULOUSE AVIATION DATA PLATFORM
# ============================================================

import json
import os
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, TopicPartition
from google.cloud import storage


# ============================================================
# 1. CONFIGURATION
# ============================================================

TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "aircraft_positions",
)

GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "aviation-gcs-writers",
)

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

BUCKET_NAME = os.getenv(
    "AVIATION_RAW_BUCKET",
    "toulouse-aviation-data-raw",
)

BATCH_SIZE = int(
    os.getenv(
        "KAFKA_BATCH_SIZE",
        "3",
    )
)

MAX_WAIT_SECONDS = int(
    os.getenv(
        "KAFKA_MAX_WAIT_SECONDS",
        "10",
    )
)


# ============================================================
# 2. CLIENT GCS
# ============================================================

gcs_client = storage.Client()

bucket = gcs_client.bucket(
    BUCKET_NAME
)


# ============================================================
# 3. CONSUMER KAFKA
# ============================================================

consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",

        # Commit automatique désactivé.
        #
        # Le commit est effectué uniquement
        # après un upload GCS réussi.
        "enable.auto.commit": False,
    }
)


# ============================================================
# 4. ABONNEMENT AU TOPIC
# ============================================================

consumer.subscribe(
    [TOPIC]
)


# ============================================================
# 5. BUFFERS
# ============================================================

# Données JSON destinées au fichier JSONL.
buffer = []

# Messages Kafka correspondants.
#
# On les conserve pour connaître leur partition
# et leur offset au moment du commit.
kafka_messages = []

# Heure d'arrivée du premier événement du batch.
#
# None = aucun batch en cours.
batch_start_time = None


# ============================================================
# 6. CONSTRUCTION DU JSONL
# ============================================================

def build_jsonl(events):
    """
    Convertit plusieurs événements Python
    en contenu JSONL.

    Une ligne JSON = un événement.
    """

    lines = []

    for event in events:
        json_line = json.dumps(
            event
        )

        lines.append(
            json_line
        )

    return "\n".join(
        lines
    )


# ============================================================
# 7. UPLOAD GCS
# ============================================================

def upload_batch_to_gcs(events):
    """
    Transforme un batch d'événements en JSONL
    et crée un objet dans GCS RAW.

    Retourne l'URI GCS créée.
    """

    now = datetime.now(
        timezone.utc
    )

    object_path = (
        "raw/streaming/aircraft_positions/"
        f"year={now:%Y}/"
        f"month={now:%m}/"
        f"day={now:%d}/"
        f"aircraft_positions_"
        f"{now:%Y%m%d_%H%M%S_%f}.jsonl"
    )

    jsonl_content = build_jsonl(
        events
    )

    blob = bucket.blob(
        object_path
    )

    blob.upload_from_string(
        jsonl_content,
        content_type="application/x-ndjson",
    )

    gcs_uri = (
        f"gs://{BUCKET_NAME}/"
        f"{object_path}"
    )

    return gcs_uri


# ============================================================
# 8. CALCUL DES OFFSETS À COMMITTER
# ============================================================

def build_commit_positions(messages):
    """
    Calcule les offsets Kafka à valider
    pour toutes les partitions présentes
    dans le batch.

    Kafka mémorise le prochain offset à lire.
    """

    offsets_to_commit = {}

    for kafka_message in messages:

        partition = (
            kafka_message.partition()
        )

        next_offset = (
            kafka_message.offset()
            + 1
        )

        if (
            partition not in offsets_to_commit
            or next_offset
            > offsets_to_commit[partition]
        ):
            offsets_to_commit[
                partition
            ] = next_offset

    commit_positions = []

    for (
        partition,
        offset,
    ) in offsets_to_commit.items():

        commit_positions.append(
            TopicPartition(
                TOPIC,
                partition,
                offset,
            )
        )

    return commit_positions


# ============================================================
# 9. FLUSH DU BATCH
# ============================================================

def flush_batch():
    """
    Traite complètement le batch courant :

    buffer
      ↓
    JSONL
      ↓
    GCS
      ↓
    commit Kafka
      ↓
    nettoyage des buffers

    Le commit Kafka n'est effectué que si
    l'upload GCS a réussi.
    """

    global batch_start_time

    if not buffer:
        return

    print(
        "\n========================================"
    )
    print(
        "FLUSH DU BATCH"
    )
    print(
        "========================================"
    )

    print(
        f"Nombre d'événements : "
        f"{len(buffer)}"
    )

    # --------------------------------------------------------
    # 1. UPLOAD GCS
    # --------------------------------------------------------

    print(
        "Création du fichier JSONL..."
    )

    gcs_uri = upload_batch_to_gcs(
        buffer
    )

    print(
        "Upload GCS réussi."
    )
    print(
        f"Objet créé : {gcs_uri}"
    )

    # --------------------------------------------------------
    # 2. CALCUL DES OFFSETS
    # --------------------------------------------------------

    commit_positions = (
        build_commit_positions(
            kafka_messages
        )
    )

    # --------------------------------------------------------
    # 3. COMMIT KAFKA
    # --------------------------------------------------------

    # Cette ligne n'est atteinte que si
    # l'upload GCS a réussi.

    consumer.commit(
        offsets=commit_positions,
        asynchronous=False,
    )

    print(
        "Offsets Kafka du batch validés."
    )

    for position in commit_positions:

        print(
            f"  Partition "
            f"{position.partition} "
            f"→ prochain offset "
            f"{position.offset}"
        )

    # --------------------------------------------------------
    # 4. NETTOYAGE
    # --------------------------------------------------------

    buffer.clear()

    kafka_messages.clear()

    batch_start_time = None

    print(
        "Buffers vidés."
    )
    print(
        "========================================\n"
    )


# ============================================================
# 10. DÉMARRAGE
# ============================================================

print(
    "========================================"
)
print(
    "Kafka -> GCS Consumer"
)
print(
    "========================================"
)

print(
    f"Bootstrap      : {BOOTSTRAP_SERVERS}"
)
print(
    f"Topic          : {TOPIC}"
)
print(
    f"Consumer Group : {GROUP_ID}"
)
print(
    f"Batch size     : {BATCH_SIZE}"
)
print(
    f"Max wait       : {MAX_WAIT_SECONDS}s"
)
print(
    f"Bucket GCS     : {BUCKET_NAME}"
)

print(
    "\nEn attente d'événements..."
)
print(
    "Ctrl + C pour arrêter."
)


# ============================================================
# 11. BOUCLE PRINCIPALE
# ============================================================

try:

    while True:

        # ----------------------------------------------------
        # Lecture Kafka
        # ----------------------------------------------------

        message = consumer.poll(
            1.0
        )

        # ----------------------------------------------------
        # Aucun nouveau message
        # ----------------------------------------------------

        if message is None:

            if (
                buffer
                and batch_start_time is not None
            ):

                elapsed_time = (
                    time.time()
                    - batch_start_time
                )

                if (
                    elapsed_time
                    >= MAX_WAIT_SECONDS
                ):

                    print(
                        f"\nTimeout atteint "
                        f"({elapsed_time:.1f}s)."
                    )

                    print(
                        f"Flush du batch partiel : "
                        f"{len(buffer)} événement(s)."
                    )

                    flush_batch()

            continue

        # ----------------------------------------------------
        # Erreur Kafka
        # ----------------------------------------------------

        if message.error():

            print(
                f"Erreur Kafka : "
                f"{message.error()}"
            )

            continue

        # ----------------------------------------------------
        # Conversion du message
        # ----------------------------------------------------

        value = (
            message
            .value()
            .decode("utf-8")
        )

        event = json.loads(
            value
        )

        # ----------------------------------------------------
        # Ajout au buffer
        # ----------------------------------------------------

        buffer.append(
            event
        )

        kafka_messages.append(
            message
        )

        if batch_start_time is None:
            batch_start_time = (
                time.time()
            )

        print(
            f"Événement ajouté au buffer : "
            f"{event.get('icao24')} "
            f"(partition={message.partition()}, "
            f"offset={message.offset()}) "
            f"({len(buffer)}/{BATCH_SIZE})"
        )

        # ----------------------------------------------------
        # Test de la taille du batch
        # ----------------------------------------------------

        if len(buffer) >= BATCH_SIZE:

            print(
                "\nTaille maximale du batch atteinte."
            )

            flush_batch()


# ============================================================
# 12. ARRÊT MANUEL
# ============================================================

except KeyboardInterrupt:

    print(
        "\nArrêt demandé par l'utilisateur."
    )


# ============================================================
# 13. FERMETURE
# ============================================================

finally:

    consumer.close()

    print(
        "Consumer Kafka fermé proprement."
    )