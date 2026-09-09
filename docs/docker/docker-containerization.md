# Module 9 — Docker & Containerisation

## 1. Objectif

L'objectif de ce module est de comprendre et mettre en pratique Docker dans le cadre de la Toulouse Aviation Data Platform.

Docker est utilisé pour rendre les composants de la plateforme :

- reproductibles ;
- isolés ;
- configurables ;
- facilement déployables ;
- indépendants de la machine du développeur.

Dans le projet, Docker est notamment utilisé pour exécuter :

- Kafka ;
- le producer Kafka Python ;
- le consumer Kafka Python.

Docker Compose permet ensuite d'orchestrer ces différents containers.

---

# 2. Dockerfile, image et container

Un Dockerfile décrit comment construire une image.

Exemple :

```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD ["python", "app.py"]
```

Le workflow est :

```text
Dockerfile
    ↓ docker build
Image Docker
    ↓ docker run
Container
```

## Image

Une image est un modèle immuable contenant notamment :

- le système minimal ;
- les dépendances ;
- le code ;
- la commande de démarrage.

## Container

Un container est une instance créée à partir d'une image.

Une même image peut servir à créer plusieurs containers.

Analogie :

```text
Image     ≈ classe
Container ≈ instance
```

---

# 3. Commandes Docker fondamentales

Lister les containers actifs :

```bash
docker ps
```

Lister tous les containers :

```bash
docker ps -a
```

Lister les images :

```bash
docker images
```

Construire une image :

```bash
docker build -t aviation-python-demo:1.0 .
```

Lancer un container :

```bash
docker run aviation-python-demo:1.0
```

Supprimer un container :

```bash
docker rm <container>
```

Supprimer automatiquement le container après son exécution :

```bash
docker run --rm aviation-python-demo:1.0
```

---

# 4. Cycle de vie d'un container

Un container reste actif tant que son processus principal fonctionne.

Par exemple :

```bash
docker run hello-world
```

exécute le programme puis le container passe immédiatement à l'état :

```text
Exited (0)
```

Cela ne signifie pas qu'il y a eu une erreur.

`0` indique au contraire que le processus s'est terminé correctement.

Un consumer Kafka, en revanche, est généralement un processus long-running et reste actif :

```text
Up
```

---

# 5. Persistance des données

Les données écrites dans le filesystem interne d'un container peuvent disparaître lorsque le container est supprimé.

Docker fournit notamment deux mécanismes importants.

## Named volume

Exemple :

```bash
docker volume create aviation-demo-data
```

Puis :

```bash
docker run -it \
  -v aviation-demo-data:/data \
  ubuntu bash
```

Le volume est géré par Docker.

Il est particulièrement adapté aux données persistantes comme celles d'une base de données.

## Bind mount

Exemple :

```bash
docker run --rm -it \
  -v "$(pwd):/app" \
  ubuntu bash
```

Ici, un dossier de la machine hôte est directement monté dans le container.

C'est particulièrement pratique en développement.

### Différence

```text
Named volume
Docker gère l'emplacement physique des données.

Bind mount
Un répertoire précis de l'hôte est partagé avec le container.
```

---

# 6. Layers et cache Docker

Les instructions d'un Dockerfile génèrent des couches.

Docker peut réutiliser les couches déjà construites.

Pour une application Python, on préfère :

```dockerfile
COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .
```

à :

```dockerfile
COPY . .

RUN pip install -r requirements.txt
```

La raison est que les dépendances changent généralement moins souvent que le code.

Si seul le code Python change, Docker peut conserver en cache :

```text
COPY requirements.txt
RUN pip install
```

et ne reconstruire que les couches suivantes.

Cela accélère fortement les builds.

---

# 7. Build context et .dockerignore

Lors d'un :

```bash
docker build .
```

le `.` représente le build context envoyé à Docker.

Il est inutile et potentiellement dangereux d'envoyer des fichiers comme :

- `.git` ;
- `.env` ;
- caches Python ;
- credentials ;
- fichiers temporaires.

Un `.dockerignore` permet de les exclure.

Exemple :

```text
__pycache__
*.pyc
.env
.git
```

Cela permet :

- de réduire le build context ;
- d'accélérer les builds ;
- d'éviter d'inclure accidentellement des secrets dans une image.

---

# 8. Variables d'environnement

La configuration ne doit pas être codée en dur dans l'application.

Exemple Python :

```python
import os

bootstrap_servers = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)
```

En local :

```text
localhost:9092
```

Dans Docker Compose :

```text
kafka:29092
```

La même application et la même image peuvent donc fonctionner dans plusieurs environnements sans modifier le code.

Principe :

```text
Code identique
+
Configuration différente
=
Environnements différents
```

---

# 9. Mapping des ports

Syntaxe :

```bash
docker run -p HOST:CONTAINER image
```

Exemple :

```bash
docker run -p 8081:8000 image
```

signifie :

```text
Machine hôte :8081
        ↓
Container :8000
```

Le service devient donc accessible depuis l'hôte via :

```text
localhost:8081
```

Règle à retenir :

```text
-p HOST:CONTAINER
```

---

# 10. Réseau Docker

Docker permet aux containers présents sur le même réseau de communiquer entre eux.

Dans Docker Compose, les services peuvent se joindre en utilisant leur nom de service.

Exemple :

```text
producer
   ↓
kafka:29092
   ↓
Kafka
```

Le producer n'utilise donc pas :

```text
localhost:29092
```

mais :

```text
kafka:29092
```

Dans un container :

```text
localhost
```

désigne le container lui-même.

---

# 11. Docker Compose

Docker Compose permet de décrire plusieurs services dans un fichier YAML.

Dans la Toulouse Aviation Data Platform :

```text
Docker Compose
│
├── Kafka
│
├── Producer
│
└── Consumer
```

Cela permet de démarrer l'environnement de manière cohérente.

Commandes principales :

```bash
docker compose config
```

Valide et affiche la configuration.

```bash
docker compose up -d
```

Démarre les services.

```bash
docker compose ps
```

Affiche leur état.

```bash
docker compose logs
```

Affiche les logs.

```bash
docker compose down
```

Arrête et supprime les containers et réseaux Compose.

---

# 12. Kafka et Docker

Kafka doit être accessible depuis deux contextes différents.

Depuis la machine hôte :

```text
localhost:9092
```

Depuis les containers Docker :

```text
kafka:29092
```

Deux listeners sont donc utilisés.

Conceptuellement :

```text
WSL / Host
    |
localhost:9092
    |
    v
+---------+
|  Kafka  |
+---------+
    ^
    |
kafka:29092
    |
Producer / Consumer Docker
```

---

# 13. Containerisation du producer Kafka

Le producer Python est construit avec un Dockerfile dédié.

Exemple :

```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY ingestion/kafka/producer.py .

RUN pip install --no-cache-dir confluent-kafka==2.6.1

CMD ["python", "producer.py"]
```

Construction :

```bash
docker build \
  -f docker/kafka-producer/Dockerfile \
  -t aviation-kafka-producer:1.0 \
  .
```

Dans Docker Compose :

```yaml
producer:
  image: aviation-kafka-producer:1.0

  environment:
    KAFKA_BOOTSTRAP_SERVERS: kafka:29092
```

Le producer est un job court :

```text
démarrage
   ↓
production des événements
   ↓
fin
   ↓
Exited (0)
```

Il n'a donc pas besoin d'une politique de redémarrage permanente.

---

# 14. Containerisation du consumer Kafka

Le consumer possède également son Dockerfile.

```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY ingestion/kafka/consumer.py .

RUN pip install --no-cache-dir confluent-kafka==2.6.1

CMD ["python", "consumer.py"]
```

Le consumer est un service long-running.

Il attend continuellement de nouveaux événements Kafka.

Il est donc pertinent d'utiliser :

```yaml
restart: unless-stopped
```

---

# 15. Logs Python et PYTHONUNBUFFERED

Python peut bufferiser ses sorties standard.

Dans un container, cela peut empêcher l'affichage immédiat des `print()` dans :

```bash
docker compose logs consumer
```

Nous utilisons donc :

```yaml
environment:
  PYTHONUNBUFFERED: "1"
```

Cela force Python à envoyer immédiatement les sorties vers les logs du container.

À ne pas confondre avec le healthcheck :

```text
PYTHONUNBUFFERED
→ gestion de la sortie Python / logs

healthcheck
→ vérification de l'état du service
```

---

# 16. Healthcheck Kafka

Le fait qu'un container soit démarré ne signifie pas nécessairement que l'application qu'il contient est prête.

Kafka peut avoir besoin de plusieurs secondes pour s'initialiser.

Un healthcheck permet de vérifier réellement son état.

Exemple :

```yaml
healthcheck:
  test:
    [
      "CMD-SHELL",
      "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1"
    ]
  interval: 5s
  timeout: 5s
  retries: 10
  start_period: 10s
```

Docker peut alors distinguer :

```text
Starting
↓
Healthy
```

---

# 17. depends_on et service_healthy

Une simple dépendance :

```yaml
depends_on:
  - kafka
```

garantit essentiellement l'ordre de démarrage.

Elle ne garantit pas que Kafka soit réellement prêt.

Nous utilisons donc :

```yaml
depends_on:
  kafka:
    condition: service_healthy
```

Ainsi :

```text
Kafka démarre
     ↓
Healthcheck
     ↓
Kafka healthy
     ↓
Producer / Consumer démarrent
```

Différence fondamentale :

```text
service_started
→ container démarré

service_healthy
→ application vérifiée comme disponible
```

---

# 18. Politique de restart

Les politiques de redémarrage doivent correspondre au type de workload.

## Consumer

Le consumer est un service permanent.

```yaml
restart: unless-stopped
```

est pertinent.

## Producer

Notre producer synthétique est un job court.

Une fois les événements envoyés, il doit se terminer.

Lui appliquer automatiquement une politique de redémarrage pourrait provoquer de nouvelles exécutions et republier les événements.

L'idempotence Kafka du producer ne signifie pas qu'un script peut être relancé arbitrairement sans jamais créer de doublons métier.

---

# 19. Version pinning

Éviter :

```yaml
image: apache/kafka:latest
```

Préférer une version explicite :

```yaml
image: apache/kafka:4.3.1
```

Cela garantit une meilleure reproductibilité.

Avec `latest`, une reconstruction future pourrait récupérer une version différente et introduire :

- incompatibilités ;
- changements de configuration ;
- nouveaux comportements ;
- régressions.

Le principe s'applique également aux dépendances Python.

---

# 20. docker compose down et volumes

Commande :

```bash
docker compose down
```

supprime les containers et réseaux Compose mais conserve normalement les volumes nommés.

En revanche :

```bash
docker compose down -v
```

supprime également les volumes associés.

Cela peut être dangereux pour des services persistants comme :

- PostgreSQL ;
- Kafka lorsqu'il utilise un volume persistant ;
- tout stockage applicatif important.

Règle :

```text
down
→ arrêter l'infrastructure

down -v
→ arrêter l'infrastructure ET supprimer ses volumes
```

---

# 21. Architecture Docker du projet

L'environnement d'ingestion peut être représenté ainsi :

```text
                    Docker Compose
                          |
        +-----------------+-----------------+
        |                 |                 |
        v                 v                 v
+---------------+   +-----------+   +---------------+
| Kafka Producer|-->|   Kafka   |-->| Kafka Consumer|
|    Python     |   |           |   |    Python     |
+---------------+   +-----------+   +---------------+
        |                 ^                 |
        |                 |                 |
        +------ kafka:29092 ----------------+
```

Depuis l'hôte :

```text
WSL / Host
    |
    +---- localhost:9092 ----> Kafka
```

---

# 22. Bonnes pratiques appliquées

Le projet applique plusieurs bonnes pratiques Docker :

- images versionnées ;
- dépendances Python versionnées ;
- séparation code/configuration ;
- variables d'environnement ;
- réseau interne Docker ;
- healthcheck Kafka ;
- démarrage conditionné à l'état healthy ;
- politique de restart adaptée au workload ;
- `.dockerignore` ;
- cache des layers optimisé ;
- Dockerfiles dédiés aux composants ;
- orchestration avec Docker Compose.

---

# 23. Points de vigilance

## Ne pas utiliser localhost entre containers

Incorrect :

```text
producer → localhost:29092
```

Correct :

```text
producer → kafka:29092
```

## Ne pas utiliser latest sans raison

Préférer :

```text
apache/kafka:4.3.1
```

## Ne pas stocker la configuration dans le code

Préférer :

```python
os.environ.get(...)
```

## Ne pas supprimer les volumes sans vérifier

Attention à :

```bash
docker compose down -v
```

## Container démarré ≠ application prête

Utiliser un healthcheck lorsque la disponibilité réelle du service est nécessaire.

---

# 24. Commandes utiles pour le projet

Construire les images :

```bash
docker build \
  -f docker/kafka-producer/Dockerfile \
  -t aviation-kafka-producer:1.0 \
  .

docker build \
  -f docker/kafka-consumer/Dockerfile \
  -t aviation-kafka-consumer:1.0 \
  .
```

Valider Compose :

```bash
docker compose config
```

Démarrer :

```bash
docker compose up -d
```

Voir les services :

```bash
docker compose ps
```

Logs Kafka :

```bash
docker compose logs kafka
```

Logs producer :

```bash
docker compose logs producer
```

Logs consumer :

```bash
docker compose logs consumer
```

Arrêter :

```bash
docker compose down
```

---

# 25. Réponse type entretien

Question :

> Comment avez-vous utilisé Docker dans votre projet ?

Réponse :

> J'ai utilisé Docker pour rendre l'environnement d'ingestion Kafka reproductible. J'ai conteneurisé séparément le producer et le consumer Python avec leurs Dockerfiles, puis utilisé Docker Compose pour orchestrer Kafka et les différents clients. Les containers communiquent via le réseau Docker en utilisant le nom du service Kafka plutôt que localhost. J'ai ajouté un healthcheck afin que les clients ne démarrent que lorsque Kafka est réellement disponible. Enfin, j'ai adapté les politiques de redémarrage au type de workload, avec un consumer long-running et un producer exécuté comme un job.

---

# 26. Compétences acquises

À l'issue du module, les compétences suivantes ont été pratiquées :

- comprendre Dockerfile, image et container ;
- construire et exécuter une image ;
- comprendre le cycle de vie d'un container ;
- utiliser bind mounts et named volumes ;
- optimiser le cache des layers ;
- utiliser `.dockerignore` ;
- injecter une configuration par variables d'environnement ;
- exposer et mapper des ports ;
- comprendre les réseaux Docker ;
- faire communiquer plusieurs containers ;
- utiliser Docker Compose ;
- conteneuriser des applications Python ;
- intégrer Kafka dans Docker Compose ;
- mettre en place un healthcheck ;
- gérer les dépendances entre services ;
- configurer les politiques de restart ;
- versionner explicitement les images ;
- diagnostiquer un environnement avec `docker ps`, `logs` et `compose ps`.

---

# 27. Points à retenir pour révision

```text
Dockerfile → docker build → Image → docker run → Container
```

```text
-p HOST:CONTAINER
```

```text
Entre containers :
kafka:29092

Depuis l'hôte :
localhost:9092
```

```text
service_started
≠
service_healthy
```

```text
PYTHONUNBUFFERED=1
→ logs Python immédiats
```

```text
docker compose down
→ conserve les volumes

docker compose down -v
→ peut supprimer les données persistantes
```

```text
Version explicite
→ reproductibilité

latest
→ comportement futur non maîtrisé
```

---

# 28. Bilan du module

Le module Docker a permis de passer d'une exécution locale des composants Kafka à une architecture conteneurisée et orchestrée.

La Toulouse Aviation Data Platform dispose désormais des bases nécessaires pour exécuter son ingestion de manière reproductible :

```text
Producer Python
      ↓
    Kafka
      ↓
Consumer Python
```

avec :

```text
Dockerfiles
+
Docker Compose
+
Networking
+
Variables d'environnement
+
Healthchecks
+
Restart policies
+
Version pinning
```

Ces fondations seront utilisées dans le module suivant pour mettre en place l'automatisation des contrôles, builds et validations avec CI/CD.