# BigQuery — Toulouse Aviation Data Platform

## 1. Objectif

BigQuery constitue la couche analytique de la Toulouse Aviation Data Platform.

Les données brutes sont d'abord conservées dans Google Cloud Storage (GCS), puis chargées dans BigQuery afin de permettre :

- les requêtes SQL analytiques ;
- les agrégations ;
- les transformations futures avec dbt ;
- l'exploitation des données par Power BI.

Architecture :

GCS RAW → BigQuery → dbt → Power BI

---

## 2. Dataset BigQuery

Projet GCP :

`toulouse-aviation-data`

Dataset :

`aviation_raw`

Localisation :

`europe-west9 (Paris)`

Le dataset est situé dans la même région que le bucket GCS utilisé pour les données RAW.

---

## 3. Table aircraft_positions

Table :

`toulouse-aviation-data.aviation_raw.aircraft_positions`

La table est une table BigQuery native.

Schéma :

| Colonne | Type |
|---|---|
| icao24 | STRING |
| callsign | STRING |
| latitude | FLOAT |
| longitude | FLOAT |
| altitude | FLOAT |
| timestamp | TIMESTAMP |

Les colonnes sont actuellement NULLABLE.

---

## 4. GCS RAW et BigQuery

GCS et BigQuery ont des responsabilités différentes.

### GCS RAW

GCS conserve les données brutes reçues par la plateforme.

Cette couche permet notamment :

- de conserver les données originales ;
- de rejouer un traitement ;
- de reconstruire les données BigQuery en cas d'erreur ;
- de séparer stockage brut et stockage analytique.

### BigQuery

BigQuery fournit une représentation structurée et optimisée pour les traitements analytiques SQL.

---

## 5. Format JSONL

Les positions sont chargées depuis GCS au format JSONL / NDJSON.

Règle :

`1 ligne physique = 1 objet JSON = 1 enregistrement`

Exemple :

```json
{"icao24":"39abcd","callsign":"AFR123","altitude":11000}
{"icao24":"4ca123","callsign":"RYR456","altitude":9000}

## 6. Partitionnement

La table `aircraft_positions` est partitionnée quotidiennement sur :

`timestamp`

L'objectif est de permettre à BigQuery d'éliminer les partitions inutiles lorsqu'une requête filtre une période.

Exemple :

```sql
WHERE timestamp >= TIMESTAMP('2026-09-01')
  AND timestamp < TIMESTAMP('2026-09-02')
```

Ce mécanisme est appelé **partition pruning**.

Il permet de réduire le volume de données scannées et donc d'améliorer les performances et de maîtriser les coûts.

---

## 7. Clustering

La table est clusterisée sur :

`icao24`

Cette colonne représente l'identifiant ICAO24 de l'aéronef.

Le clustering est pertinent car les requêtes peuvent régulièrement rechercher les positions d'un avion particulier :

```sql
WHERE icao24 = '39abcd'
```

BigQuery peut ainsi réduire les blocs de données à scanner lorsqu'une requête filtre sur `icao24`.

---

## 8. Optimisation des requêtes

Trois mécanismes principaux ont été étudiés.

### Sélection des colonnes

Éviter `SELECT *` lorsque toutes les colonnes ne sont pas nécessaires.

Exemple :

```sql
SELECT
    icao24,
    callsign,
    altitude
FROM `toulouse-aviation-data.aviation_raw.aircraft_positions`;
```

BigQuery utilisant un stockage colonnaire, sélectionner uniquement les colonnes utiles permet de limiter les données traitées.

### Partitionnement

Filtrer sur `timestamp` permet à BigQuery d'éliminer les partitions inutiles.

### Clustering

Filtrer sur `icao24` permet de bénéficier de l'organisation des données clusterisées.

---

## 9. SQL analytique

Les opérations SQL suivantes ont été pratiquées :

- `SELECT`
- `WHERE`
- `COUNT`
- `AVG`
- `MIN`
- `MAX`
- `GROUP BY`
- `HAVING`
- `ORDER BY`

Différence importante :

`WHERE` filtre les lignes **avant** l'agrégation.

`HAVING` filtre les groupes **après** l'agrégation.

Exemple :

```sql
SELECT
    icao24,
    AVG(altitude) AS altitude_moyenne
FROM `toulouse-aviation-data.aviation_raw.aircraft_positions`
WHERE altitude >= 9000
GROUP BY icao24
HAVING AVG(altitude) > 10000;
```

---

## 10. Détection des doublons

Les doublons peuvent être recherchés avec `GROUP BY`, `COUNT` et `HAVING`.

Exemple :

```sql
SELECT
    icao24,
    callsign,
    timestamp,
    COUNT(*) AS nombre_occurrences
FROM `toulouse-aviation-data.aviation_raw.aircraft_positions`
GROUP BY
    icao24,
    callsign,
    timestamp
HAVING COUNT(*) > 1;
```

Cette requête permet d'identifier plusieurs occurrences d'une même position logique.

---

## 11. Compte de service BigQuery

Un compte de service dédié a été créé :

`aviation-bigquery-loader`

Sa responsabilité est de charger les données RAW présentes dans GCS vers BigQuery.

Les permissions ont été limitées selon le **principe du moindre privilège**.

### GCS

Rôle :

`Storage Object Viewer`

Responsabilité :

Lire les objets RAW présents dans le bucket :

`toulouse-aviation-data-raw`

### Projet GCP

Rôle :

`BigQuery Job User`

Responsabilité :

Lancer des jobs BigQuery.

### Dataset aviation_raw

Rôle :

`BigQuery Data Editor`

Responsabilité :

Écrire dans les tables du dataset `aviation_raw`.

Cette permission est appliquée au dataset plutôt qu'à l'ensemble des datasets du projet.

---

## 12. Authentification

Le code Python ne contient aucune clé de compte de service.

Les **Application Default Credentials (ADC)** sont utilisées avec l'impersonation du compte :

`aviation-bigquery-loader`

La distinction importante est :

**ADC = authentification / identité**

ADC permet à l'application de déterminer sous quelle identité elle communique avec Google Cloud.

**IAM = autorisation / permissions**

IAM détermine ce que cette identité est autorisée à effectuer.

Le compte utilisateur dispose également du rôle :

`Service Account Token Creator`

sur `aviation-bigquery-loader`.

Cela permet de générer un jeton temporaire et d'impersonner le compte de service sans utiliser de clé JSON permanente.

---

## 13. Loader Python

Le loader est situé dans :

`ingestion/bigquery/bigquery_loader.py`

Il utilise la bibliothèque :

```python
from google.cloud import bigquery
```

Le client est créé avec :

```python
client = bigquery.Client(project=PROJECT_ID)
```

Grâce à ADC, aucune clé n'est directement présente dans le programme.

Le chargement GCS vers BigQuery est réalisé avec :

```python
load_job = client.load_table_from_uri(
    GCS_URI,
    table_id,
    job_config=job_config
)
```

Le programme attend ensuite la fin du job avec :

```python
load_job.result()
```

Un Load Job étant asynchrone, `result()` permet d'attendre sa terminaison et de récupérer une éventuelle erreur.

---

## 14. WRITE_APPEND

Le Load Job utilise :

```python
write_disposition=bigquery.WriteDisposition.WRITE_APPEND
```

`WRITE_APPEND` signifie que les nouvelles lignes sont ajoutées aux données déjà présentes.

Pendant le module :

```text
Table initiale : 7 lignes
Nouveau fichier : 6 lignes
Résultat : 13 lignes
```

Le chargement Python GCS → BigQuery a donc été validé.

---

## 15. Idempotence

Le loader actuel n'est pas encore **idempotent**.

Avec `WRITE_APPEND`, relancer plusieurs fois exactement le même fichier ajoute plusieurs fois les mêmes données.

Par exemple :

```text
Fichier contenant 6 lignes

1 exécution  → +6
2 exécutions → +12
10 exécutions → +60
```

Le loader ne vérifie actuellement pas si une donnée a déjà été chargée.

Cette problématique devra être traitée dans les couches de transformation et d'industrialisation.

Des mécanismes tels que :

- la déduplication ;
- le suivi des fichiers déjà traités ;
- `MERGE` ;
- UPSERT ;

pourront être utilisés selon l'architecture retenue.

---

## 16. Pipeline obtenu

Le pipeline fonctionnel à la fin du module est :

```text
aircraft_positions_sample.jsonl
            ↓
       GCS RAW
            ↓
aviation-bigquery-loader
            ↓
   BigQuery Load Job
            ↓
aviation_raw.aircraft_positions
```

Le chargement **GCS → BigQuery est maintenant automatisable en Python** et ne nécessite plus une création manuelle du Load Job dans BigQuery Studio.

---

## 17. Séparation des responsabilités IAM

Deux comptes de service sont maintenant utilisés.

### aviation-gcs-writer

Responsabilité :

```text
Source / application
        ↓
aviation-gcs-writer
        ↓
GCS RAW
```

Il peut créer de nouveaux objets RAW.

Il ne peut pas :

- lire les objets ;
- supprimer les objets ;
- remplacer les objets existants.

### aviation-bigquery-loader

Responsabilité :

```text
GCS RAW
        ↓
aviation-bigquery-loader
        ↓
BigQuery
```

Il peut :

- lire les données RAW ;
- lancer un job BigQuery ;
- écrire dans `aviation_raw`.

Cette séparation évite d'utiliser un compte de service disposant de permissions trop larges.

---

## 18. Architecture obtenue à ce stade

```text
Positions avions
       ↓
Application / ingestion
       ↓
aviation-gcs-writer
       ↓
GCS RAW
       ↓
aviation-bigquery-loader
       ↓
BigQuery Load Job
       ↓
aviation_raw.aircraft_positions
       ↓
Future couche dbt
       ↓
Power BI
```

La prochaine étape du projet sera la transformation des données BigQuery avec **dbt**.

---

## 19. Compétences acquises

À l'issue du module BigQuery :

- création d'un dataset BigQuery ;
- création et compréhension d'une table native ;
- définition d'un schéma ;
- types `STRING`, `FLOAT` et `TIMESTAMP` ;
- compréhension de JSON vs JSONL ;
- chargement GCS → BigQuery ;
- `WRITE_APPEND` ;
- partitionnement temporel ;
- partition pruning ;
- clustering ;
- optimisation des données scannées ;
- stockage colonnaire ;
- SQL analytique ;
- `WHERE` vs `HAVING` ;
- agrégations ;
- détection des doublons ;
- BigQuery Load Jobs ;
- client Python BigQuery ;
- IAM et principe du moindre privilège ;
- comptes de service dédiés ;
- ADC ;
- impersonation ;
- notion d'idempotence.

---

## 20. Résultat

**Module 3 — BigQuery : VALIDÉ**

Pipeline fonctionnel :

`GCS RAW → Python BigQuery Loader → BigQuery`

Prochaine étape :

**Module 4 — dbt**