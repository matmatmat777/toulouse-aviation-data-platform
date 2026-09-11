import os

from confluent_kafka import Consumer


# ============================================================
# 1. CONFIGURATION
# ============================================================

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "aircraft_positions",
)

GROUP_ID = os.getenv(
    "KAFKA_TEST_GROUP_ID",
    "aviation-rebalance-test",
)


consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
)


# ============================================================
# 2. ABONNEMENT AU TOPIC
# ============================================================

consumer.subscribe(
    [TOPIC]
)


print(
    "Consumer démarré."
)

print(
    f"Kafka bootstrap : {BOOTSTRAP_SERVERS}"
)

print(
    f"Écoute du topic : {TOPIC}"
)

print(
    f"Consumer Group : {GROUP_ID}"
)

print(
    "Commit automatique : désactivé"
)

print(
    "Commit manuel : désactivé pour "
    "le test de rebalancing"
)

print(
    "Ctrl + C pour arrêter."
)


# ============================================================
# 3. BOUCLE DE CONSOMMATION
# ============================================================

try:

    while True:

        message = consumer.poll(
            1.0
        )

        # ----------------------------------------------------
        # Aucun message
        # ----------------------------------------------------

        if message is None:
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
        # Récupération de la key
        # ----------------------------------------------------

        key = (
            message.key().decode("utf-8")
            if message.key()
            else None
        )

        # ----------------------------------------------------
        # Récupération de la value
        # ----------------------------------------------------

        value = (
            message
            .value()
            .decode("utf-8")
        )

        # ----------------------------------------------------
        # Affichage
        # ----------------------------------------------------

        print(
            "\n----------------------------------------"
        )

        print(
            "Événement reçu"
        )

        print(
            "----------------------------------------"
        )

        print(
            f"Key       : {key}"
        )

        print(
            f"Partition : {message.partition()}"
        )

        print(
            f"Offset    : {message.offset()}"
        )

        print(
            f"Value     : {value}"
        )

        # ----------------------------------------------------
        # Commit manuel
        # ----------------------------------------------------
        #
        # Désactivé volontairement pendant
        # le test de rebalancing.
        #
        # consumer.commit(
        #     message=message,
        #     asynchronous=False,
        # )


# ============================================================
# 4. ARRÊT
# ============================================================

except KeyboardInterrupt:

    print(
        "\nArrêt demandé par l'utilisateur."
    )


# ============================================================
# 5. FERMETURE PROPRE
# ============================================================

finally:

    consumer.close()

    print(
        "Consumer Kafka fermé proprement."
    )