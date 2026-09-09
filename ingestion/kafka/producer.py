from confluent_kafka import Producer
import json
import time
import os


# ============================================================
# 1. CONFIGURATION DU PRODUCER
# ============================================================

producer = Producer({
    "bootstrap.servers": os.environ.get(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092"
    ),

    "enable.idempotence": True
})


TOPIC = "aircraft_positions"


# ============================================================
# 2. CALLBACK DE LIVRAISON
# ============================================================

def delivery_report(err, msg):
    """
    Fonction appelée par Kafka quand la livraison
    d'un message a réussi ou échoué.
    """

    if err is not None:

        print(
            f"Échec de livraison : {err}"
        )

    else:

        print(
            "Message livré ✅ "
            f"topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


# ============================================================
# 3. ÉVÉNEMENTS À ENVOYER
# ============================================================

aircraft_positions = [
    {
        "icao24": "39abcd",
        "callsign": "AFR123",
        "latitude": 43.6047,
        "longitude": 1.4442,
        "altitude": 11000,
        "timestamp": "2026-09-03T13:00:00Z"
    },
    {
        "icao24": "39abcd",
        "callsign": "AFR123",
        "latitude": 43.6100,
        "longitude": 1.4500,
        "altitude": 11500,
        "timestamp": "2026-09-03T13:01:00Z"
    },
    {
        "icao24": "4ca123",
        "callsign": "RYR456",
        "latitude": 43.5800,
        "longitude": 1.4200,
        "altitude": 9000,
        "timestamp": "2026-09-03T13:02:00Z"
    }
]


# ============================================================
# 4. ENVOI DES MESSAGES
# ============================================================

for position in aircraft_positions:

    producer.produce(

        topic=TOPIC,

        # La key détermine la partition.
        #
        # Tous les événements d'un même avion
        # utilisent la même key ICAO24.
        key=position["icao24"],

        # Kafka transporte ici notre JSON
        # sous forme de chaîne.
        value=json.dumps(position),

        # Fonction appelée lorsque Kafka confirme
        # ou refuse la livraison.
        callback=delivery_report
    )


    print(
        f"Événement mis en file d'envoi : "
        f"{position['icao24']} - "
        f"{position['timestamp']}"
    )


    # Permet au producer de traiter les callbacks
    # disponibles sans bloquer.
    producer.poll(0)

    time.sleep(1)


# ============================================================
# 5. FLUSH FINAL
# ============================================================

# Attend que tous les messages encore présents
# dans le buffer interne du producer soient traités.
producer.flush()


print("Tous les événements ont été traités par le producer.")