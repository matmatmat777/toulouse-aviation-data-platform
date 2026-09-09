# ============================================================
# CONSUMER KAFKA - TOULOUSE AVIATION DATA PLATFORM
# ============================================================
#
# Objectif :
#   Lire les événements de positions d'avions présents
#   dans le topic Kafka "aircraft_positions".
#
# Pour ce test :
#
#   Kafka
#     ↓
#   Consumer Python
#     ↓
#   affichage dans le terminal
#
# Le commit manuel est volontairement désactivé
# pour observer le comportement du Consumer Group
# et le rebalancing.
#
# ============================================================


from confluent_kafka import Consumer
import os


# ============================================================
# 1. CONFIGURATION
# ============================================================

TOPIC = "aircraft_positions"

GROUP_ID = "aviation-rebalance-test"


consumer = Consumer({

    # Adresse du broker Kafka.
    "bootstrap.servers": os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
    ),
    # Consumer Group utilisé pour le test de rebalancing.
    "group.id": GROUP_ID,

    # Comme ce groupe est nouveau,
    # on lit depuis les messages les plus anciens disponibles.
    "auto.offset.reset": "earliest",

    # Commit automatique désactivé.
    "enable.auto.commit": False
})


# ============================================================
# 2. ABONNEMENT AU TOPIC
# ============================================================

consumer.subscribe([TOPIC])


print("Consumer démarré.")
print(f"Écoute du topic : {TOPIC}")
print(f"Consumer Group : {GROUP_ID}")
print("Commit automatique : désactivé")
print("Commit manuel : désactivé pour le test de rebalancing")
print("Ctrl + C pour arrêter.")


# ============================================================
# 3. BOUCLE DE CONSOMMATION
# ============================================================

try:

    while True:

        # ----------------------------------------------------
        # POLL
        # ----------------------------------------------------
        #
        # Le consumer demande à Kafka :
        #
        # "As-tu un message pour moi ?"
        #
        # Il attend au maximum 1 seconde.
        #

        message = consumer.poll(1.0)


        # ----------------------------------------------------
        # AUCUN MESSAGE
        # ----------------------------------------------------

        if message is None:
            continue


        # ----------------------------------------------------
        # ERREUR KAFKA
        # ----------------------------------------------------

        if message.error():

            print(
                f"Erreur Kafka : "
                f"{message.error()}"
            )

            continue


        # ====================================================
        # 4. RÉCUPÉRATION DE LA KEY
        # ============================================================

        key = (
            message.key().decode("utf-8")
            if message.key()
            else None
        )


        # ====================================================
        # 5. RÉCUPÉRATION DE LA VALUE
        # ============================================================

        value = message.value().decode("utf-8")


        # ====================================================
        # 6. AFFICHAGE DU MESSAGE
        # ============================================================

        print("\n----------------------------------------")
        print("Événement reçu")
        print("----------------------------------------")

        print(f"Key       : {key}")
        print(f"Partition : {message.partition()}")
        print(f"Offset    : {message.offset()}")
        print(f"Value     : {value}")


        # ====================================================
        # 7. COMMIT MANUEL
        # ============================================================
        #
        # Désactivé volontairement pendant
        # le test de rebalancing.
        #
        # Sinon :
        #
        # consumer.commit(
        #     message=message,
        #     asynchronous=False
        # )
        #
        # print("Offset validé (commit).")


# ============================================================
# 8. ARRÊT AVEC CTRL + C
# ============================================================

except KeyboardInterrupt:

    print("\nArrêt demandé par l'utilisateur.")


# ============================================================
# 9. FERMETURE PROPRE
# ============================================================

finally:

    consumer.close()

    print("Consumer Kafka fermé proprement.")