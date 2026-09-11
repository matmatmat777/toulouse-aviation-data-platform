import json
import os
import time

from confluent_kafka import Producer


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


producer = Producer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "enable.idempotence": True,
    }
)


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
        "timestamp": "2026-09-03T13:00:00Z",
    },
    {
        "icao24": "39abcd",
        "callsign": "AFR123",
        "latitude": 43.6100,
        "longitude": 1.4500,
        "altitude": 11500,
        "timestamp": "2026-09-03T13:01:00Z",
    },
    {
        "icao24": "4ca123",
        "callsign": "RYR456",
        "latitude": 43.5800,
        "longitude": 1.4200,
        "altitude": 9000,
        "timestamp": "2026-09-03T13:02:00Z",
    },
]


# ============================================================
# 4. DÉMARRAGE
# ============================================================

print(
    f"Kafka bootstrap : {BOOTSTRAP_SERVERS}"
)

print(
    f"Topic           : {TOPIC}"
)


# ============================================================
# 5. ENVOI DES MESSAGES
# ============================================================

for position in aircraft_positions:

    producer.produce(
        topic=TOPIC,

        # Tous les événements d'un même avion
        # utilisent la même key ICAO24.
        key=position["icao24"],

        value=json.dumps(
            position
        ),

        callback=delivery_report,
    )

    print(
        f"Événement mis en file d'envoi : "
        f"{position['icao24']} - "
        f"{position['timestamp']}"
    )

    producer.poll(0)

    time.sleep(1)


# ============================================================
# 6. FLUSH FINAL
# ============================================================

producer.flush()

print(
    "Tous les événements ont été traités "
    "par le producer."
)