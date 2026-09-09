# Module 2 — Google Cloud Platform Fundamentals

## Objectif

Mettre en place la première infrastructure Google Cloud de la **Toulouse Aviation Data Platform**.

Ce module couvre :

- Google Cloud Project
- Google Cloud Storage
- IAM
- Service Accounts
- Application Default Credentials (ADC)
- Service Account Impersonation
- Principe du moindre privilège

---

## 1. Projet Google Cloud

Projet utilisé :

`Toulouse Aviation Data`

Project ID :

`toulouse-aviation-data`

Le projet GCP constitue le périmètre principal dans lequel sont organisées les ressources Cloud de la plateforme.

Il permet notamment de regrouper :

- les ressources GCS ;
- les identités et permissions IAM ;
- les futurs services BigQuery ;
- les futurs composants de traitement et d'orchestration.

---

## 2. Zone RAW — Google Cloud Storage

Bucket :

`toulouse-aviation-data-raw`

Localisation :

`europe-west9 (Paris)`

Classe de stockage :

`Standard`

La zone **RAW** conserve les données telles qu'elles arrivent dans la plateforme.

Les données RAW ne doivent pas être modifiées directement.

Elles permettent notamment :

- de rejouer un traitement ;
- de reconstruire les données transformées ;
- de conserver la donnée source ;
- de faciliter l'analyse d'un problème dans le pipeline.

---

## 3. Organisation des objets

Les positions d'avions sont stockées selon une organisation temporelle :

```text
raw/
└── streaming/
    └── aircraft_positions/
        └── year=YYYY/
            └── month=MM/
                └── day=DD/
                    └── positions_YYYYMMDD_HHMMSS_microseconds.json
```

Exemple :

```text
gs://toulouse-aviation-data-raw/
raw/streaming/aircraft_positions/
year=2026/month=09/day=01/
positions_20260901_160530_123456.json
```

Dans Google Cloud Storage, les « dossiers » affichés dans la console correspondent principalement à une organisation logique basée sur le nom des objets.

Il faut donc distinguer :

```text
Project
   ↓
Bucket
   ↓
Object
```

Dans notre projet :

```text
Project
toulouse-aviation-data

        ↓

Bucket
toulouse-aviation-data-raw

        ↓

Object
raw/streaming/aircraft_positions/
year=2026/month=09/day=01/
positions_20260901_160530_123456.json
```

---

## 4. Service Account

Service Account utilisé pour l'écriture RAW :

`aviation-gcs-writer`

Adresse :

`aviation-gcs-writer@toulouse-aviation-data.iam.gserviceaccount.com`

Un **Service Account** est une identité technique destinée à être utilisée par un programme ou un workload.

Dans notre architecture, cette identité sera utilisée par le composant chargé d'écrire les positions d'avions dans GCS.

On distingue donc :

```text
Utilisateur humain
→ compte Google du développeur

Service Account
→ identité d'une application ou d'un workload
```

---

## 5. IAM et principe du moindre privilège

IAM signifie :

**Identity and Access Management**

IAM permet de déterminer :

> Qui peut faire quoi sur quelle ressource ?

On peut représenter le principe ainsi :

```text
Principal
   +
Rôle
   +
Ressource
```

Dans notre cas :

```text
Principal
aviation-gcs-writer

        +

Rôle
Storage Object Creator

        +

Ressource
toulouse-aviation-data-raw
```

Le Service Account possède sur le bucket RAW le rôle :

`Storage Object Creator`

L'objectif est d'appliquer le **principe du moindre privilège**.

Le writer doit pouvoir créer de nouveaux objets RAW, mais il n'a pas besoin de lire, modifier ou supprimer les objets existants.

Permissions testées :

| Action | Résultat |
|---|---|
| Création d'un nouvel objet | ✅ Autorisée |
| Lecture d'un objet | ❌ Refusée |
| Remplacement d'un objet existant | ❌ Refusé |
| Suppression d'un objet | ❌ Refusée |

Cette restriction protège la zone RAW contre les modifications ou suppressions accidentelles effectuées par le composant d'ingestion.

---

## 6. Authentication vs Authorization

Il faut distinguer deux notions.

### Authentication

L'authentication répond à la question :

> Qui es-tu ?

Exemple :

```text
Je suis :
aviation-gcs-writer
```

### Authorization

L'authorization répond à la question :

> Qu'as-tu le droit de faire ?

Exemple :

```text
aviation-gcs-writer

CREATE → autorisé
READ   → refusé
DELETE → refusé
```

Dans notre architecture :

```text
Authentication
      ↓
identité
      ↓
aviation-gcs-writer
      ↓
IAM
      ↓
Authorization
      ↓
actions autorisées
```

---

## 7. Application Default Credentials — ADC

Le code Python utilise :

```python
client = storage.Client()
```

Aucun login, mot de passe ou fichier de clé privée n'est stocké directement dans le code.

La bibliothèque Google Cloud utilise **Application Default Credentials (ADC)** pour rechercher automatiquement les credentials disponibles dans l'environnement d'exécution.

Il faut surtout distinguer **ADC et IAM**.

```text
ADC
→ détermine les credentials / l'identité utilisée

IAM
→ détermine ce que cette identité est autorisée à faire
```

ADC n'est donc pas un rôle administrateur.

ADC ne donne pas automatiquement tous les privilèges.

Les permissions dépendent toujours des rôles IAM attribués à l'identité utilisée.

---

## 8. Service Account Impersonation

Pour le développement local, le Service Account est utilisé via **impersonation**.

L'impersonation permet à un utilisateur autorisé d'agir temporairement avec l'identité d'un Service Account.

Architecture d'authentification utilisée pendant le développement :

```text
Développeur
    │
    │ ADC
    ▼
Compte utilisateur Google
    │
    │ impersonation
    ▼
aviation-gcs-writer
    │
    │ IAM
    ▼
Storage Object Creator
    │
    ▼
GCS RAW
```

Le compte utilisateur possède sur le Service Account le rôle :

`Service Account Token Creator`

Ce rôle permet d'obtenir temporairement des credentials pour agir comme :

`aviation-gcs-writer`

On distingue donc bien les deux rôles utilisés :

```text
Service Account Token Creator
        ↓
permet au développeur
d'impersonner le Service Account


Storage Object Creator
        ↓
permet au Service Account
de créer des objets dans GCS
```

Cette approche évite de stocker une clé privée permanente de Service Account dans le repository.

---

## 9. Configuration ADC avec impersonation

L'environnement local a été configuré avec :

```powershell
gcloud auth application-default login --impersonate-service-account=aviation-gcs-writer@toulouse-aviation-data.iam.gserviceaccount.com
```

Les credentials ADC sont ensuite utilisés automatiquement par les bibliothèques Google Cloud.

Le programme Python peut donc simplement utiliser :

```python
from google.cloud import storage

client = storage.Client()
```

La chaîne complète devient :

```text
Python
   ↓
google-cloud-storage
   ↓
ADC
   ↓
Compte utilisateur
   ↓
Impersonation
   ↓
aviation-gcs-writer
   ↓
IAM
   ↓
Google Cloud Storage
```

---

## 10. Validation IAM — Test CREATE

Premier test :

le programme tente de créer un nouvel objet dans GCS.

Résultat :

```text
CREATE → SUCCESS
```

Exemple :

```text
positions_python_test_03.json
```

L'objet a correctement été créé dans :

```text
gs://toulouse-aviation-data-raw/
raw/streaming/aircraft_positions/
year=2026/month=09/day=01/
```

Cela confirme que :

```text
aviation-gcs-writer
        ↓
Storage Object Creator
        ↓
CREATE autorisé
```

---

## 11. Validation IAM — Test de remplacement

Un premier objet existait déjà :

```text
positions_python_test.json
```

Le programme a tenté d'écrire de nouveau sur le même nom.

Résultat :

```text
403 Forbidden
```

L'erreur indiquait notamment :

```text
storage.objects.delete
```

Le Service Account ne possède pas cette permission.

Cela démontre qu'il ne peut pas remplacer librement un objet RAW existant.

```text
Objet existant
      ↓
tentative de remplacement
      ↓
storage.objects.delete nécessaire
      ↓
permission absente
      ↓
403 Forbidden
```

---

## 12. Validation IAM — Test READ

Le programme a ensuite tenté de lire un objet existant avec :

```python
content = blob_to_read.download_as_text()
```

Résultat :

```text
403 Forbidden
```

L'erreur indiquait :

```text
Permission 'storage.objects.get' denied
```

Cela démontre que le Service Account ne possède pas la permission permettant de lire le contenu d'un objet.

```text
aviation-gcs-writer

CREATE  → ✅
READ    → ❌
DELETE  → ❌
REPLACE → ❌
```

Le principe du moindre privilège est donc effectivement appliqué.

---

## 13. Writer Python

Le composant :

```text
ingestion/gcs/gcs_writer.py
```

est responsable de l'écriture des événements dans la zone RAW.

Sa responsabilité est volontairement limitée :

```text
Événement Python
      ↓
Sérialisation JSON
      ↓
Construction du chemin temporel
      ↓
Upload
      ↓
Google Cloud Storage
      ↓
Zone RAW
```

Le composant utilise :

```python
json.dumps(data)
```

pour transformer un dictionnaire Python en JSON.

Exemple :

```text
Dictionnaire Python
        ↓
json.dumps()
        ↓
JSON
        ↓
upload_from_string()
        ↓
GCS
```

---

## 14. Construction dynamique des chemins

Le chemin GCS est construit automatiquement à partir de l'heure UTC.

Exemple :

```python
object_path = (
    "raw/streaming/aircraft_positions/"
    f"year={now:%Y}/"
    f"month={now:%m}/"
    f"day={now:%d}/"
    f"positions_{now:%Y%m%d_%H%M%S_%f}.json"
)
```

Cela permet d'obtenir automatiquement une organisation telle que :

```text
year=2026/
month=09/
day=01/
positions_20260901_160530_123456.json
```

Le `%f` ajoute les microsecondes.

Cela réduit fortement le risque que deux événements utilisent exactement le même nom d'objet.

C'est particulièrement important puisque le writer n'est volontairement pas autorisé à remplacer un objet RAW existant.

---

## 15. Utilisation de l'UTC

Les timestamps du pipeline sont générés en UTC :

```python
from datetime import datetime, timezone

now = datetime.now(timezone.utc)
```

L'utilisation de l'UTC permet d'avoir une référence temporelle commune indépendamment :

- du fuseau horaire du développeur ;
- du pays ;
- de l'aéroport ;
- de la machine exécutant le pipeline.

C'est particulièrement pertinent pour une plateforme manipulant des données aéronautiques internationales.

---

## 16. Fonction réutilisable

La logique d'écriture est encapsulée dans :

```python
def upload_aircraft_position(data: dict) -> str:
```

Cette fonction :

```text
reçoit
↓
un dictionnaire Python

transforme
↓
les données en JSON

construit
↓
le chemin GCS

upload
↓
l'objet RAW

retourne
↓
l'URI gs://...
```

Plus tard, un autre composant pourra simplement utiliser :

```python
upload_aircraft_position(event)
```

Le writer GCS devient ainsi un composant réutilisable.

---

## 17. `if __name__ == "__main__"`

Le fichier contient également :

```python
if __name__ == "__main__":
```

Ce bloc permet de distinguer deux situations.

### Exécution directe

Si on exécute :

```powershell
python ingestion\gcs\gcs_writer.py
```

alors :

```text
__name__ == "__main__"
```

et le code de test local est exécuté.

### Import du module

Si un autre programme fait :

```python
from ingestion.gcs.gcs_writer import upload_aircraft_position
```

le bloc de test n'est pas exécuté automatiquement.

Cela permettra plus tard à un consumer d'importer la fonction :

```python
upload_aircraft_position()
```

sans créer automatiquement un faux événement de test.

---

## 18. Sécurité des credentials

Aucune clé privée de Service Account n'est stockée dans le repository.

Le projet utilise :

```text
ADC
+
Service Account Impersonation
```

plutôt qu'un fichier de clé JSON permanent.

Le repository ne doit jamais contenir :

```text
credentials.json
service-account-key.json
clé privée
mot de passe
token d'accès
```

Les credentials et secrets doivent rester séparés du code source.

---

## 19. Architecture obtenue à la fin du module

À la fin du Module 2, le flux opérationnel est :

```text
Aircraft Position
       │
       ▼
Python
gcs_writer.py
       │
       │ google-cloud-storage
       ▼
ADC
       │
       ▼
Service Account Impersonation
       │
       ▼
aviation-gcs-writer
       │
       │ IAM
       │ Storage Object Creator
       ▼
Google Cloud Storage
       │
       ▼
toulouse-aviation-data-raw
       │
       ▼
raw/streaming/aircraft_positions/
       │
       ├── year=YYYY/
       │    └── month=MM/
       │         └── day=DD/
       │
       ▼
JSON RAW
```

---

## 20. Concepts acquis

À l'issue de ce module :

- distinction entre **Project, Bucket et Object** ;
- compréhension du fonctionnement de **Google Cloud Storage** ;
- compréhension de la zone **RAW** ;
- organisation logique des objets GCS ;
- compréhension d'**IAM** ;
- distinction entre **authentication et authorization** ;
- compréhension des **Service Accounts** ;
- application du **principe du moindre privilège** ;
- compréhension d'**ADC** ;
- utilisation de la **Service Account Impersonation** ;
- utilisation de `google-cloud-storage` en Python ;
- écriture d'objets JSON dans GCS ;
- construction dynamique des chemins temporels ;
- validation expérimentale des permissions IAM.

---

## 21. Résultat du Module 2

La Toulouse Aviation Data Platform dispose maintenant de sa première brique Cloud fonctionnelle :

```text
Python
   ↓
Service Account
   ↓
IAM
   ↓
GCS
   ↓
RAW
```

Le stockage RAW est opérationnel et sécurisé par un Service Account disposant uniquement des permissions nécessaires à son rôle.

Cette couche servira de source aux futurs traitements de la plateforme.