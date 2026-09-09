# Kafka Fundamentals — Toulouse Aviation Data Platform

## 1. Objectif

Kafka est utilisé dans le projet pour gérer l'ingestion streaming des positions d'avions.

Architecture :

```text
Aviation API
    ↓
Python Producer
    ↓
Kafka
aircraft_positions
    ↓
Python Consumer
    ↓
Buffer
    ↓
JSONL
    ↓
GCS RAW
```

Kafka permet notamment :

- de découpler le Producer et les Consumers ;
- d'absorber des flux d'événements ;
- de conserver temporairement les événements ;
- de permettre le replay ;
- de paralléliser la consommation grâce aux partitions et Consumer Groups.

---

# 2. Topic

Le topic utilisé dans le projet est :

```text
aircraft_positions
```

Il représente le flux des positions successives des avions.

Le topic possède actuellement 3 partitions :

```text
aircraft_positions

├── Partition 0
├── Partition 1
└── Partition 2
```

Création :

```powershell
docker exec aviation-kafka /opt/kafka/bin/kafka-topics.sh --create --topic aircraft_positions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

---

# 3. Event et Offset

Un événement correspond à une donnée métier.

Exemple :

```json
{
  "icao24": "39abcd",
  "callsign": "AFR123",
  "latitude": 43.6047,
  "longitude": 1.4442,
  "altitude": 11000,
  "timestamp": "2026-09-03T13:00:00Z"
}
```

Un offset correspond à la position séquentielle d'un événement dans une partition.

Exemple :

```text
Partition 0

offset 0
offset 1
offset 2
offset 3
...
```

Les offsets sont propres à chaque partition.

---

# 4. Kafka Key

Le Producer utilise :

```python
key=position["icao24"]
```

Exemple :

```text
39abcd → Partition 0
39abcd → Partition 0
39abcd → Partition 0

4ca123 → Partition 1
```

L'objectif est de conserver les événements d'un même avion dans la même partition.

Kafka garantit l'ordre des événements à l'intérieur d'une partition.

La key Kafka ne doit pas être confondue avec une clé métier utilisée pour la déduplication.

Dans le projet :

```text
icao24
→ Kafka key
→ partitionnement

(icao24, timestamp)
→ clé métier possible
→ identification / déduplication

offset + commit
→ progression du Consumer Group
```

---

# 5. Producer

Le Producer Python publie les positions dans Kafka.

Configuration :

```python
producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "enable.idempotence": True
})
```

L'idempotence protège notamment contre certains doublons pouvant être provoqués par les retries entre le Producer et Kafka.

Elle ne garantit pas l'exactly-once sur l'ensemble du pipeline.

---

# 6. Delivery callback

Un callback permet de connaître le résultat réel de la livraison :

```python
def delivery_report(err, msg):

    if err is not None:
        print(f"Échec de livraison : {err}")

    else:
        print(
            f"Message livré "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )
```

Résultat observé :

```text
39abcd → partition 0 → offset 5
39abcd → partition 0 → offset 6
4ca123 → partition 1 → offset 2
```

Les événements utilisant la même key `39abcd` sont donc bien envoyés dans la même partition.

---

# 7. Consumer

Le Consumer lit les événements Kafka avec :

```python
consumer.poll(1.0)
```

Configuration principale :

```python
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "aviation-gcs-writers",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})
```

Le commit automatique est volontairement désactivé.

Le Consumer décide explicitement quand un événement peut être considéré comme traité.

---

# 8. Consumer Groups

Les Consumers appartenant au même Consumer Group se partagent les partitions.

Exemple :

```text
3 partitions
2 consumers

Consumer A → P0 + P1
Consumer B → P2
```

Une partition est attribuée à au plus un Consumer d'un même groupe à un instant donné.

Le parallélisme maximal d'un Consumer Group est donc limité par le nombre de partitions.

Exemple :

```text
3 partitions
5 consumers

3 consumers actifs
2 consumers sans partition
```

Des Consumer Groups différents peuvent lire indépendamment le même topic.

Exemple :

```text
aircraft_positions
       │
       ├── aviation-gcs-writers
       │
       └── aviation-alerting
```

Les deux groupes peuvent recevoir l'intégralité du flux indépendamment.

---

# 9. Rebalancing

Lorsqu'un Consumer rejoint ou quitte un Consumer Group, Kafka peut redistribuer les partitions.

Test réalisé :

```text
3 partitions
2 consumers

Consumer A → P0 + P1
Consumer B → P2
```

Après arrêt du Consumer B :

```text
Consumer A → P0 + P1 + P2
```

Kafka a automatiquement réattribué la partition abandonnée.

Ce mécanisme s'appelle le rebalancing.

---

# 10. Commit

Un commit indique jusqu'où un Consumer Group a traité les données.

Il ne supprime pas les événements de Kafka.

Kafka enregistre le prochain offset à lire.

Si le dernier événement traité est :

```text
offset 42
```

le commit correspondant est :

```text
43
```

Dans le code :

```python
next_offset = kafka_message.offset() + 1
```

---

# 11. Commit multi-partitions

Un batch peut contenir des messages provenant de plusieurs partitions.

Exemple :

```text
P0 → offsets 116, 117, 118
P1 → offsets 77, 78
```

Après traitement réussi :

```text
P0 → commit 119
P1 → commit 79
```

Le projet calcule donc le plus grand prochain offset pour chaque partition.

```python
offsets_to_commit = {}

for kafka_message in messages:

    partition = kafka_message.partition()
    next_offset = kafka_message.offset() + 1

    if (
        partition not in offsets_to_commit
        or next_offset > offsets_to_commit[partition]
    ):
        offsets_to_commit[partition] = next_offset
```

Puis :

```python
commit_positions.append(
    TopicPartition(
        TOPIC,
        partition,
        offset
    )
)
```

Enfin :

```python
consumer.commit(
    offsets=commit_positions,
    asynchronous=False
)
```

Cette correction a été mise en place après avoir observé qu'un commit basé uniquement sur le dernier message du batch ne validait pas correctement l'avancement de toutes les partitions concernées.

---

# 12. At-least-once

Le projet utilise une stratégie de livraison :

```text
AT-LEAST-ONCE
```

Le commit est effectué uniquement après un upload GCS réussi.

```text
Kafka
  ↓
Consumer
  ↓
GCS
  ↓
upload réussi
  ↓
commit Kafka
```

Cela privilégie la non-perte des événements.

---

# 13. Crash test

Un crash volontaire a été provoqué après l'upload GCS mais avant le commit Kafka.

Scénario :

```text
Kafka
 ↓
Consumer
 ↓
GCS écrit avec succès
 ↓
CRASH
 ↓
pas de commit
```

Au redémarrage du même Consumer Group :

```text
Kafka rejoue les événements
 ↓
nouvel upload GCS
 ↓
doublon possible
```

Le test a effectivement créé deux objets GCS contenant les mêmes événements.

Cela démontre concrètement le fonctionnement at-least-once.

La déduplication devra donc être assurée downstream, par exemple avec PySpark ou dbt.

---

# 14. JSON et JSONL

Le Producer publie un événement JSON individuel par message Kafka.

Exemple :

```json
{"icao24":"39abcd","altitude":11000}
```

Le Consumer regroupe plusieurs événements dans un fichier JSONL :

```text
{"icao24":"39abcd","altitude":11000}
{"icao24":"39abcd","altitude":11500}
{"icao24":"4ca123","altitude":9000}
```

Chaque ligne correspond à un objet JSON indépendant.

Les fichiers sont enregistrés dans GCS avec :

```text
Content-Type: application/x-ndjson
```

---

# 15. Batching

Le Consumer ne crée pas un fichier GCS pour chaque événement.

Les événements sont placés dans un buffer.

Deux conditions permettent de déclencher un flush :

```text
BATCH_SIZE atteint
OU
MAX_WAIT_SECONDS atteint
```

Configuration de test :

```python
BATCH_SIZE = 3
MAX_WAIT_SECONDS = 10
```

Cela permet de trouver un compromis entre :

- débit ;
- nombre de fichiers ;
- latence.

---

# 16. Flush par taille

Exemple :

```text
message 1 → 1/3
message 2 → 2/3
message 3 → 3/3

BATCH_SIZE atteint
        ↓
      JSONL
        ↓
       GCS
        ↓
      commit
```

---

# 17. Flush par timeout

Si le trafic est faible :

```text
message 1 → 1/3
        ↓
plus aucun événement
        ↓
10 secondes
        ↓
MAX_WAIT_SECONDS atteint
        ↓
flush du batch partiel
```

Test réalisé avec un seul événement :

```text
Événement ajouté au buffer : 39abcd (1/3)

Timeout atteint (10.2s).

Flush du batch partiel : 1 événement(s).

Upload GCS réussi.
Offsets Kafka du batch validés.
Partition 0 → prochain offset 5
```

Cela évite qu'un batch partiel reste indéfiniment en mémoire.

---

# 18. Lag

Le lag mesure le retard d'un Consumer Group.

```text
LAG = LOG-END-OFFSET - CURRENT-OFFSET
```

Exemple :

```text
CURRENT-OFFSET = 400
LOG-END-OFFSET = 500

LAG = 100
```

Un lag qui augmente continuellement peut indiquer que le Consumer n'arrive pas à suivre le rythme du Producer.

Causes possibles :

- traitement trop lent ;
- stockage destination lent ;
- nombre insuffisant de Consumers ;
- nombre insuffisant de partitions ;
- erreur applicative.

---

# 19. Replay

Lire un événement ne le supprime pas de Kafka.

Un nouveau Consumer Group configuré avec :

```python
"auto.offset.reset": "earliest"
```

peut lire les événements encore présents dans Kafka depuis le début de la rétention disponible.

Un Consumer Group existant reprend normalement depuis ses offsets commités.

---

# 20. Retention

La rétention détermine combien de temps Kafka conserve les événements.

Elle est indépendante des commits des Consumers.

```text
commit
→ progression du Consumer Group

retention
→ conservation des événements dans Kafka
```

Pour `aircraft_positions`, la conservation de l'historique est importante afin de pouvoir analyser les trajectoires.

---

# 21. Log Compaction

La compaction répond à un besoin différent.

Elle permet de conserver principalement la valeur récente associée à une key.

Exemple d'usage :

```text
aircraft_current_state

39abcd → dernière position connue
4ca123 → dernière position connue
```

Pour le topic historique :

```text
aircraft_positions
```

la rétention classique est préférable car les positions successives sont nécessaires pour reconstruire les trajectoires et effectuer des analyses historiques.

---

# 22. Broker

Un broker est un serveur Kafka.

Dans l'environnement local :

```text
Kafka Cluster
└── Broker 1
```

Le broker stocke actuellement les trois partitions du topic.

---

# 23. Leader

Chaque partition possède un leader.

Le Producer écrit vers le leader de la partition.

Dans l'environnement local :

```text
Leader: 1
```

signifie que le Broker 1 est leader.

---

# 24. Replicas

Les partitions Kafka peuvent être répliquées sur plusieurs brokers.

Exemple de production :

```text
Partition 0

Broker 1 → Leader
Broker 2 → Replica
Broker 3 → Replica
```

Si le leader tombe, un autre replica suffisamment à jour peut devenir leader.

---

# 25. ISR

ISR signifie :

```text
In-Sync Replicas
```

Ce sont les replicas suffisamment synchronisés.

Exemple :

```text
Replicas: 1,2,3
ISR:      1,2
```

Le Broker 3 possède une replica mais elle n'est actuellement pas considérée comme suffisamment synchronisée.

---

# 26. Configuration locale

Le projet utilise actuellement :

```text
1 broker
Replication Factor = 1
```

Donc :

```text
Leader:   1
Replicas: 1
ISR:      1
```

Cette configuration est suffisante pour le développement local mais n'offre pas de haute disponibilité.

Si le Broker 1 tombe, aucune autre copie n'est disponible.

---

# 27. Acknowledgments — acks

Les acknowledgments déterminent quand le Producer considère une écriture comme confirmée.

Conceptuellement :

```text
acks=0
→ aucune confirmation attendue

acks=1
→ confirmation du leader

acks=all
→ niveau de durabilité maximal selon les replicas in-sync
   et la configuration du cluster
```

`acks=all` ne crée pas de réplication.

Avec :

```text
1 broker
Replication Factor = 1
```

il n'existe toujours qu'une seule copie de la donnée.

---

# 28. Producer idempotent

Le Producer utilise :

```python
"enable.idempotence": True
```

Objectif :

```text
Producer
   ↓
Kafka reçoit le message
   ↓
ACK perdu
   ↓
Producer retry
   ↓
idempotence
   ↓
éviter le doublon lié au retry
```

Cette idempotence concerne la frontière :

```text
Producer → Kafka
```

Elle ne protège pas contre :

```text
Kafka → Consumer → GCS → crash avant commit
```

Dans ce deuxième cas, la stratégie at-least-once peut toujours produire un doublon downstream.

---

# 29. DLQ — Dead Letter Queue

Un événement invalide ne doit idéalement pas bloquer tout le pipeline.

Exemple :

```text
aircraft_positions
        ↓
     Consumer
      /     \
 valide     invalide
   ↓           ↓
 GCS          DLQ
```

Une DLQ peut contenir :

```text
message original
type d'erreur
topic source
partition
offset
timestamp
```

Exemple de topic :

```text
aircraft_positions_dlq
```

Cela facilite l'audit, le diagnostic et le retraitement.

---

# 30. Schema Registry

Le JSON est suffisant pour le projet pédagogique, mais une architecture Kafka industrialisée peut utiliser :

```text
Avro
Protobuf
Schema Registry
```

Le Schema Registry permet notamment de gérer le contrat des événements et son évolution entre Producers et Consumers.

Il devient particulièrement intéressant lorsque plusieurs équipes ou applications partagent les mêmes flux Kafka.

---

# 31. Sécurité

L'environnement local utilise :

```text
PLAINTEXT://localhost:9092
```

C'est adapté au développement local.

Une architecture de production doit traiter trois problématiques :

```text
TLS
→ chiffrement des communications

SASL
→ authentification

ACL
→ autorisation
```

Les ACL suivent une logique proche du least privilege utilisé avec IAM sur GCP.

Exemple :

```text
aviation-producer
→ WRITE aircraft_positions

aviation-gcs-consumer
→ READ aircraft_positions

monitoring
→ permissions de consultation
```

---

# 32. Monitoring

Le lag est une métrique essentielle.

Exemple :

```text
Producer → 1000 événements/s
Consumer → 400 événements/s

LAG augmente
```

Une réponse possible consiste à augmenter le parallélisme des Consumers.

Cependant :

```text
parallélisme maximal
≈ nombre de partitions
```

Avec 3 partitions, ajouter 10 Consumers dans le même groupe ne permet pas à 10 Consumers de traiter simultanément le topic.

---

# 33. Architecture Kafka finale

```text
Aviation API
      │
      ▼
Python Producer
      │
      │ key = icao24
      │ idempotence
      ▼
Kafka
aircraft_positions
      │
 ┌────┼────┐
 │    │    │
P0   P1   P2
 │    │    │
 └────┼────┘
      │
      ▼
Consumer Group
aviation-gcs-writers
      │
      ▼
Buffer
      │
 ┌────┴─────────────┐
 │                  │
BATCH_SIZE       MAX_WAIT
atteint           atteint
 │                  │
 └────────┬─────────┘
          ▼
        JSONL
          │
          ▼
       GCS RAW
          │
     upload réussi
          │
          ▼
commit offsets
par partition
```

---

# 34. Garantie de traitement

Le compromis choisi est :

```text
AT-LEAST-ONCE
+
déduplication downstream
```

Raison :

```text
perdre un événement
→ non souhaité

rejouer un événement
→ acceptable si déduplication
```

Le pipeline privilégie donc la fiabilité et le replay au prix de doublons potentiels.

---

# 35. Concepts acquis

À l'issue de ce module :

- Producer ;
- Consumer ;
- Broker ;
- Topic ;
- Partition ;
- Kafka key ;
- Offset ;
- Commit ;
- Consumer Group ;
- Rebalancing ;
- Lag ;
- Replay ;
- Retention ;
- Log compaction ;
- Leader ;
- Replica ;
- ISR ;
- acknowledgments ;
- retries ;
- Producer idempotent ;
- at-least-once ;
- batching ;
- timeout ;
- JSONL ;
- commit multi-partitions ;
- DLQ ;
- Schema Registry ;
- TLS ;
- SASL ;
- ACL ;
- monitoring Kafka.

---

# 36. Résultat

Le projet possède désormais une ingestion streaming fonctionnelle :

```text
Python Producer
      ↓
Kafka
      ↓
Consumer
      ↓
batch taille OU timeout
      ↓
JSONL
      ↓
GCS RAW
      ↓
commit Kafka
```

Les principaux comportements de fiabilité ont été testés expérimentalement :

- partitionnement par `icao24` ;
- offsets ;
- commit manuel ;
- replay ;
- Consumer Groups ;
- rebalancing ;
- lag ;
- crash avant commit ;
- at-least-once ;
- doublons ;
- batching ;
- timeout ;
- commit multi-partitions ;
- Producer idempotent.

Le stockage RAW produit par cette couche servira d'entrée au prochain composant de la plateforme :

```text
GCS RAW
   ↓
PySpark
   ↓
nettoyage
validation
déduplication
transformation
   ↓
BigQuery
```