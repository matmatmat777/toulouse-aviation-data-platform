# Databricks & Delta Lake — Toulouse Aviation Data Platform

## Objectif du module

Ce module met en œuvre une architecture **Lakehouse** basée sur **Apache Spark** et **Delta Lake** pour la Toulouse Aviation Data Platform.

L'objectif est de comprendre et d'implémenter les mécanismes fondamentaux utilisés dans une plateforme Databricks moderne :

- architecture Lakehouse ;
- architecture Medallion ;
- tables Delta ;
- transactions ACID ;
- `_delta_log` ;
- Time Travel ;
- MERGE / UPSERT ;
- Schema Enforcement ;
- Schema Evolution ;
- Data Quality ;
- déduplication ;
- Bronze / Silver / Gold ;
- traitement incrémental ;
- OPTIMIZE ;
- VACUUM ;
- partitionnement ;
- Data Skipping ;
- cache Spark ;
- Unity Catalog ;
- Databricks SQL Warehouse ;
- Databricks Workflows.

Les traitements ont été réalisés localement avec **PySpark + Delta Lake** afin de comprendre les mécanismes fondamentaux avant leur utilisation dans un environnement Databricks managé.

---

# 1. Spark, Databricks et Delta Lake

Ces trois technologies ont des rôles différents.

## Apache Spark

Apache Spark est un **moteur de calcul distribué**.

Il permet notamment :

- de lire de gros volumes de données ;
- de transformer les données ;
- d'effectuer des jointures ;
- d'effectuer des agrégations ;
- d'exécuter les traitements en parallèle ;
- de distribuer le calcul sur plusieurs workers/executors.

Exemple :

```python
df = (
    spark.read
    .json("aircraft_positions.jsonl")
)

df.filter(
    "altitude > 10000"
).show()
```

Spark est donc principalement responsable du **calcul**.

---

## Delta Lake

Delta Lake apporte une **couche de table transactionnelle** au-dessus de fichiers de données, généralement au format Parquet.

Une table Delta repose principalement sur :

```text
TABLE DELTA
│
├── fichiers Parquet
│   └── données
│
└── _delta_log/
    └── transactions / versions / métadonnées
```

Delta Lake apporte notamment :

- transactions ACID ;
- versionnement ;
- Time Travel ;
- MERGE / UPSERT ;
- Schema Enforcement ;
- Schema Evolution ;
- historique des opérations.

Delta Lake n'est pas un moteur de calcul : Spark reste le moteur qui exécute les transformations.

---

## Databricks

Databricks est une **plateforme Data/Lakehouse**.

Elle fournit notamment :

- du compute Spark ;
- des notebooks ;
- des jobs ;
- des workflows ;
- des tables Delta ;
- Databricks SQL ;
- des SQL Warehouses ;
- Unity Catalog ;
- des fonctionnalités de gouvernance ;
- du monitoring.

On peut résumer :

```text
Spark
→ moteur de calcul

Delta Lake
→ couche de table transactionnelle

Databricks
→ plateforme Data/Lakehouse

Unity Catalog
→ gouvernance
```

---

# 2. Data Lake, Data Warehouse et Lakehouse

## Data Lake

Un Data Lake permet de stocker de gros volumes de données sous différents formats.

Il est particulièrement adapté aux données :

- brutes ;
- semi-structurées ;
- structurées ;
- issues de multiples sources.

Exemples :

- JSON ;
- CSV ;
- Parquet ;
- événements Kafka.

---

## Data Warehouse

Un Data Warehouse est davantage orienté :

- données structurées ;
- SQL ;
- reporting ;
- BI ;
- analytics.

Les données y sont généralement déjà transformées et organisées pour les usages métier.

---

## Lakehouse

L'approche Lakehouse cherche à combiner les avantages du Data Lake et du Data Warehouse.

```text
DATA LAKE
flexibilité / stockage massif
          +
DATA WAREHOUSE
SQL / transactions / gouvernance
          ↓
       LAKEHOUSE
```

Delta Lake participe à cette approche en apportant notamment des propriétés transactionnelles à des données stockées dans un environnement de type Data Lake.

---

# 3. Architecture Medallion

L'architecture du projet suit trois couches principales :

```text
RAW
 ↓
BRONZE
 ↓
SILVER
 ↓
GOLD
```

Cette organisation est appelée **architecture Medallion**.

---

# 4. Couche Bronze

La couche Bronze conserve les données **proches de leur format source**.

Dans le projet :

```text
data/delta/bronze/aircraft_positions
```

Les données Bronze peuvent encore contenir :

- types incorrects ;
- valeurs invalides ;
- valeurs manquantes ;
- doublons ;
- données nécessitant une transformation.

Exemple observé dans le TP :

```text
altitude = "PAS_UN_NOMBRE"
```

Le schéma Bronze obtenu était notamment :

```text
altitude             string
callsign             string
icao24               string
ingestion_timestamp  string
latitude             double
longitude            double
timestamp             string
aircraft_type         string
```

Le fait que `altitude` soit un `string` vient notamment du fait que la source contenait une valeur :

```text
PAS_UN_NOMBRE
```

La couche Bronze privilégie donc la **traçabilité de la donnée source** plutôt qu'une validation métier immédiate.

---

# 5. Structure physique d'une table Delta

Une table Delta n'est pas un fichier unique.

Elle correspond à un répertoire contenant notamment :

```text
aircraft_positions/
│
├── _delta_log/
│   ├── 00000000000000000000.json
│   ├── 00000000000000000001.json
│   ├── 00000000000000000002.json
│   └── ...
│
├── part-xxxxx.parquet
├── part-yyyyy.parquet
└── ...
```

Les rôles sont différents :

```text
Parquet
→ contient les données

_delta_log
→ décrit les transactions et les versions de la table
```

Le `_delta_log` **ne contient pas toutes les données de la table**.

Il contient les informations permettant à Delta de déterminer quels fichiers constituent l'état d'une version donnée.

---

# 6. Delta Transaction Log

Chaque opération modifiant une table Delta crée une nouvelle version transactionnelle.

Exemples d'opérations rencontrées :

```text
WRITE
MERGE
```

L'historique peut être consulté avec :

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(
    spark,
    path,
)

delta_table.history().show(
    truncate=False
)
```

Dans le TP Silver, l'historique a par exemple contenu :

```text
version 0 → WRITE
version 1 → MERGE
```

Cela permet de savoir comment la table a évolué.

---

# 7. ACID

Delta Lake apporte les propriétés **ACID**.

## Atomicity

Une transaction est appliquée entièrement ou pas du tout.

```text
transaction
   ↓
tout réussit
   OU
rien n'est validé
```

---

## Consistency

Une transaction doit faire passer la table d'un état cohérent à un autre état cohérent.

---

## Isolation

Les transactions concurrentes ne doivent pas exposer des états intermédiaires incohérents.

C'est notamment la propriété à laquelle on pense lorsqu'on parle de plusieurs transactions travaillant simultanément sur une table.

---

## Durability

Une transaction validée doit rester persistante.

---

# 8. Time Travel

Delta Lake conserve plusieurs versions d'une table.

Il est possible de lire une version précédente.

Exemple PySpark :

```python
df = (
    spark.read
    .format("delta")
    .option("versionAsOf", 2)
    .load(path)
)
```

Dans le TP Bronze, plusieurs versions ont été observées :

```text
Version 0 → 7 lignes
Version 1 → 7 lignes
Version 2 → 10 lignes
```

Le Time Travel permet notamment :

- d'analyser une ancienne version ;
- de comparer plusieurs états ;
- de comprendre une modification ;
- de faciliter certaines investigations.

Attention :

**Time Travel n'est pas automatiquement un système de sauvegarde.**

Les anciennes versions nécessitent toujours les fichiers physiques correspondant à ces versions.

---

# 9. MERGE / UPSERT

`MERGE` permet de réaliser des traitements incrémentaux.

Le principe utilisé dans le projet est :

```text
clé trouvée
→ UPDATE

clé absente
→ INSERT
```

La clé logique choisie pour identifier une position est :

```text
icao24 + timestamp
```

Exemple :

```python
(
    target.alias("target")
    .merge(
        source.alias("source"),
        """
        target.icao24 = source.icao24
        AND target.timestamp = source.timestamp
        """
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

---

# 10. MERGE réalisé sur Bronze pour apprentissage

Un premier MERGE a été réalisé sur la Bronze afin d'étudier le fonctionnement interne de Delta.

Deux événements ont été utilisés :

```text
400abc
→ événement existant
→ UPDATE

new999
→ nouvel événement
→ INSERT
```

Le résultat de la transaction a été :

```text
1 UPDATE
1 INSERT
```

L'historique Delta a enregistré l'opération :

```text
operation = MERGE
```

Le log a également montré que Delta écrit de nouveaux fichiers et modifie l'état logique de la table plutôt que de modifier directement une cellule dans un ancien fichier Parquet.

Ce MERGE sur Bronze avait un objectif pédagogique.

Dans une architecture Medallion, les corrections et UPSERT métier sont généralement plus appropriés sur une couche Silver/curated que sur une Bronze que l'on souhaite conserver proche de la source.

---

# 11. Schema Enforcement

Delta Lake vérifie la compatibilité du schéma lors d'une écriture.

Une tentative a été réalisée pour ajouter une colonne :

```text
aircraft_type
```

sans autoriser l'évolution du schéma.

L'écriture a été refusée.

C'est le **Schema Enforcement**.

Il protège la table contre des modifications accidentelles de sa structure.

---

# 12. Schema Evolution

Il est possible d'autoriser explicitement une évolution du schéma.

Exemple :

```python
(
    df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(path)
)
```

Dans le TP, la colonne :

```text
aircraft_type
```

a ainsi été ajoutée.

La nouvelle ligne possédait :

```text
aircraft_type = A320
```

Les anciennes lignes ont obtenu :

```text
aircraft_type = NULL
```

La nouvelle définition du schéma a été enregistrée dans `_delta_log`.

Le schéma fait donc partie de l'état versionné de la table Delta.

---

# 13. Bronze vers Silver

La couche Silver transforme les données Bronze en données :

- correctement typées ;
- nettoyées ;
- validées ;
- dédupliquées.

Pipeline :

```text
BRONZE
   ↓
typage
   ↓
Data Quality
   ↓
déduplication
   ↓
SILVER
```

Chemin Silver :

```text
data/delta/silver/aircraft_positions
```

---

# 14. Conversion tolérante avec try_cast

Dans Bronze, `altitude` était un `string`.

Exemples :

```text
"10000.0"
"-500.0"
NULL
"PAS_UN_NOMBRE"
```

La conversion Silver utilise :

```python
.withColumn(
    "altitude",
    expr("try_cast(altitude AS DOUBLE)")
)
```

Résultat :

```text
"10000.0"       → 10000.0
"-500.0"        → -500.0
NULL            → NULL
"PAS_UN_NOMBRE" → NULL
```

`try_cast` évite qu'une conversion impossible fasse nécessairement échouer l'ensemble du traitement.

La Data Quality peut ensuite décider si la ligne doit être acceptée ou rejetée.

Il faut distinguer :

```text
conversion technique
≠
validation métier
```

Par exemple :

```text
"-500.0"
```

est parfaitement convertible en `DOUBLE` :

```text
-500.0
```

mais cette valeur est rejetée par la règle métier :

```python
col("altitude") >= 0
```

---

# 15. Typage Silver

Les transformations appliquées comprennent notamment :

```text
altitude
STRING → DOUBLE

timestamp
STRING → TIMESTAMP

ingestion_timestamp
STRING → TIMESTAMP
```

Le schéma Silver obtenu est :

```text
altitude             double
callsign             string
icao24               string
ingestion_timestamp  timestamp
latitude             double
longitude            double
timestamp             timestamp
aircraft_type         string
```

---

# 16. Data Quality

Les règles appliquées à la Silver comprennent notamment :

```text
icao24
→ obligatoire

timestamp
→ obligatoire

ingestion_timestamp
→ obligatoire

latitude
→ obligatoire
→ entre -90 et 90

longitude
→ obligatoire
→ entre -180 et 180

altitude
→ obligatoire
→ >= 0
```

Trois lignes invalides ont été éliminées lors du TP :

```text
bad001
→ altitude négative

bad002
→ altitude manquante

bad003
→ altitude impossible à convertir
```

---

# 17. Quarantine / Rejected

En production, une donnée invalide ne devrait pas simplement disparaître.

Architecture cible :

```text
BRONZE
   │
   ↓
DATA QUALITY
   │
   ├── VALID
   │     ↓
   │   SILVER
   │
   └── INVALID
         ↓
      REJECTED
      QUARANTINE
```

Une zone Rejected peut conserver :

- les données originales ;
- `rejection_reason` ;
- `ingestion_timestamp` ;
- les informations techniques de traçabilité.

Exemple :

```text
icao24 = bad003
altitude = PAS_UN_NOMBRE
rejection_reason = INVALID_OR_MISSING_ALTITUDE
```

Cette stratégie permet :

- d'investiguer les erreurs ;
- d'identifier un problème de source ;
- de corriger un pipeline ;
- de rejouer éventuellement les données.

---

# 18. Déduplication Silver

La clé logique utilisée est :

```text
icao24 + timestamp
```

Pour plusieurs lignes correspondant au même événement, la ligne possédant le :

```text
ingestion_timestamp
```

le plus récent est conservée.

Une Window Spark est utilisée :

```python
window = (
    Window
    .partitionBy(
        "icao24",
        "timestamp",
    )
    .orderBy(
        col("ingestion_timestamp").desc()
    )
)
```

Puis :

```python
silver_df = (
    valid_df
    .withColumn(
        "row_num",
        row_number().over(window)
    )
    .filter(
        col("row_num") == 1
    )
    .drop(
        "row_num"
    )
)
```

Dans le TP :

```text
Bronze                         12 lignes
Valid avant déduplication      9 lignes
Silver                         8 lignes
```

Pour le doublon :

```text
39abcd / 13:00
```

la ligne conservée était celle avec :

```text
altitude = 10100
ingestion_timestamp = 13:00:08
```

---

# 19. Traitement incrémental Silver

Une reconstruction complète peut être réalisée avec :

```python
.mode("overwrite")
```

mais ce n'est pas nécessairement la stratégie souhaitée lorsque les volumes deviennent importants.

En production, on peut préférer un traitement incrémental :

```text
nouveau micro-batch
        ↓
transformation / DQ
        ↓
MERGE
        ↓
SILVER
```

---

# 20. MERGE incrémental Silver

Le TP a simulé l'arrivée de deux événements :

```text
400abc / 13:06
→ existe déjà
→ UPDATE

new888 / 13:11
→ n'existe pas
→ INSERT
```

La Silver contenait initialement :

```text
8 lignes
```

Après le MERGE :

```text
UPDATE
→ nombre de lignes inchangé

INSERT
→ +1 ligne
```

Résultat :

```text
9 lignes
```

L'historique Delta Silver a montré :

```text
version 0 → WRITE
version 1 → MERGE
```

avec :

```text
matchedPredicates
→ update

notMatchedPredicates
→ insert
```

---

# 21. ExprId Spark

Dans certains plans ou historiques Spark, les colonnes apparaissent sous une forme comme :

```text
icao24#35
timestamp#39
```

Le nombre après `#` est un **identifiant interne Spark**, appelé `ExprId`.

Par exemple :

```text
target.icao24 → icao24#35
source.icao24 → icao24#2
```

Cela permet notamment à Spark de distinguer deux expressions portant le même nom dans une jointure ou un MERGE.

Ces nombres :

- ne sont pas des valeurs ;
- ne sont pas des numéros de colonnes ;
- ne sont pas des versions Delta ;
- peuvent changer entre deux exécutions.

---

# 22. Silver vers Gold

La couche Gold est orientée :

- métier ;
- analytics ;
- reporting ;
- BI.

Chemin :

```text
data/delta/gold/airline_statistics
```

Le pipeline réalisé est :

```text
SILVER
   ↓
extraction airline_code
   ↓
jointure référentiel airlines
   ↓
agrégation
   ↓
GOLD
```

---

# 23. Extraction du code compagnie

Le code compagnie est extrait du `callsign`.

Exemples :

```text
AFR123 → AFR
RYR456 → RYR
EZY789 → EZY
BAW555 → BAW
DLH321 → DLH
```

PySpark :

```python
.withColumn(
    "airline_code",
    F.substring(
        F.col("callsign"),
        1,
        3,
    )
)
```

Le code compagnie est extrait du **callsign**, et non de `icao24`.

---

# 24. Référentiel compagnies

Le référentiel utilisé est :

```text
data/reference/airlines.csv
```

Il contient notamment :

```text
AFR → Air France
RYR → Ryanair
EZY → easyJet
BAW → British Airways
DLH → Lufthansa
```

---

# 25. LEFT JOIN

La Silver est enrichie avec le référentiel compagnies.

Un `LEFT JOIN` est utilisé.

Pourquoi ?

Parce qu'une position doit être conservée même si son code compagnie n'existe pas encore dans le référentiel.

Exemple :

```text
callsign = NEW999
airline_code = NEW
```

`NEW` n'existe pas dans le référentiel.

Résultat :

```text
airline_code = NEW
airline_name = NULL
```

La ligne est conservée.

Avec un `INNER JOIN`, elle aurait disparu.

---

# 26. Gold métier

La table Gold construite contient :

```text
airline_code
airline_name
position_count
average_altitude
min_altitude
max_altitude
```

Résultat obtenu :

```text
AFR | Air France      | 4 | 10825 | 10100 | 12500
RYR | Ryanair         | 1 | 9000  | 9000  | 9000
EZY | easyJet         | 1 | 11700 | 11700 | 11700
NEW | NULL            | 1 | 9800  | 9800  | 9800
DLH | Lufthansa       | 1 | 12000 | 12000 | 12000
BAW | British Airways | 1 | 9500  | 9500  | 9500
```

La moyenne Air France correspond à :

```text
(10100 + 10500 + 12500 + 10200) / 4
= 10825
```

La table Gold peut ensuite être consommée par une couche BI telle que Power BI.

---

# 27. Architecture Medallion obtenue

Le projet possède désormais :

```text
data/delta/
│
├── bronze/
│   └── aircraft_positions/
│
├── silver/
│   └── aircraft_positions/
│
└── gold/
    └── airline_statistics/
```

Architecture :

```text
RAW JSONL
    ↓
BRONZE DELTA
    ↓
PySpark
    ↓
typage + DQ + déduplication
    ↓
SILVER DELTA
    ↓
enrichissement + agrégation
    ↓
GOLD DELTA
```

---

# 28. OPTIMIZE

Les traitements incrémentaux et micro-batchs peuvent générer de nombreux petits fichiers.

Exemple :

```text
part-001.parquet  2 MB
part-002.parquet  3 MB
part-003.parquet  1 MB
part-004.parquet  4 MB
...
```

C'est le **small files problem**.

De nombreux petits fichiers peuvent augmenter :

- le coût de gestion des métadonnées ;
- le nombre d'opérations I/O ;
- le temps nécessaire pour planifier certaines lectures.

`OPTIMIZE` permet de réorganiser/compacter les fichiers.

Conceptuellement :

```text
AVANT

beaucoup de petits fichiers
        ↓
     OPTIMIZE
        ↓
APRES

moins de fichiers
plus volumineux
```

Les données logiques ne changent pas.

Il s'agit d'une optimisation de leur organisation physique.

---

# 29. VACUUM

Lorsqu'une table Delta évolue, certains anciens fichiers peuvent ne plus être utilisés par l'état courant de la table.

`VACUUM` permet de supprimer les anciens fichiers devenus obsolètes conformément à une politique de rétention.

Conceptuellement :

```text
ancien fichier Parquet
        ↓
nouvelle transaction
        ↓
ancien fichier plus utilisé
        ↓
rétention
        ↓
VACUUM
        ↓
suppression physique
```

Il faut être prudent.

Si un ancien fichier nécessaire à une version Time Travel est supprimé, cette ancienne version peut devenir impossible à lire.

---

# 30. Relation VACUUM / Time Travel

Le `_delta_log` peut encore indiquer :

```text
Version 2 utilisait fichier-X.parquet
```

mais si :

```text
VACUUM
```

a physiquement supprimé :

```text
fichier-X.parquet
```

Delta ne peut pas nécessairement reconstruire cette ancienne version.

Donc :

```text
historique transactionnel
≠
sauvegarde complète des données
```

---

# 31. Partitionnement Delta

Pour de gros volumes, il peut être intéressant d'organiser physiquement les données par certaines colonnes.

Exemple :

```text
flight_date=2026-09-08/
flight_date=2026-09-09/
flight_date=2026-09-10/
```

Une requête :

```sql
SELECT *
FROM aircraft_positions
WHERE flight_date = '2026-09-10';
```

peut alors éviter de lire les partitions correspondant aux autres dates.

C'est le **partition pruning**.

---

# 32. Choisir une colonne de partitionnement

Une colonne de partitionnement doit être choisie en fonction notamment :

- des volumes ;
- des requêtes ;
- de la cardinalité ;
- des filtres fréquemment utilisés.

Pour notre projet aviation :

```text
flight_date
```

peut être un choix raisonnable sur une très grosse table historique.

En revanche :

```text
icao24
```

peut avoir une très forte cardinalité.

Cela pourrait produire énormément de partitions :

```text
icao24=39abcd/
icao24=4ca123/
icao24=400abc/
icao24=406def/
...
```

et entraîner du **sur-partitionnement**.

---

# 33. repartition() vs partitionBy()

Ces deux notions ne doivent pas être confondues.

## repartition()

Exemple :

```python
df.repartition(8)
```

concerne les **partitions Spark pendant le calcul distribué**.

Spark redistribue généralement les données avec un **shuffle**.

Le nombre `8` n'est pas une valeur magique.

Le choix dépend notamment :

- du volume ;
- du nombre de cœurs ;
- du cluster ;
- du type de traitement ;
- de la taille souhaitée des partitions.

---

## write.partitionBy()

Exemple :

```python
(
    df.write
    .format("delta")
    .partitionBy("flight_date")
    .save(path)
)
```

concerne l'**organisation physique des données écrites**.

Donc :

```text
repartition()
→ partitions de calcul Spark

write.partitionBy()
→ organisation physique du stockage
```

---

# 34. repartition() vs coalesce()

`repartition()` :

```text
→ peut augmenter ou diminuer le nombre de partitions
→ provoque généralement un shuffle
→ permet de mieux redistribuer les données
```

`coalesce()` :

```text
→ surtout utilisé pour réduire le nombre de partitions
→ évite autant que possible une redistribution complète
→ moins coûteux
→ peut produire des partitions moins équilibrées
```

Exemple :

```text
100 partitions
↓
objectif : 10 partitions
```

Si on souhaite simplement réduire le nombre de partitions :

```python
df.coalesce(10)
```

peut être pertinent.

Si on souhaite rééquilibrer fortement les données :

```python
df.repartition(10)
```

peut être préférable malgré le shuffle.

Pour passer :

```text
4 partitions déséquilibrées
→ 20 partitions équilibrées
```

on utilisera :

```python
df.repartition(20)
```

---

# 35. Data Skipping

Delta peut exploiter les statistiques associées aux fichiers pour éviter de lire ceux qui ne peuvent pas satisfaire une requête.

Exemple :

```text
Fichier A
altitude min = 1000
altitude max = 5000

Fichier B
altitude min = 5100
altitude max = 9000

Fichier C
altitude min = 9100
altitude max = 13000
```

Requête :

```sql
SELECT *
FROM aircraft_positions
WHERE altitude > 10000;
```

Delta peut déterminer que :

```text
A → inutile
B → inutile
C → potentiellement utile
```

et éviter de lire certains fichiers.

C'est le **Data Skipping**.

---

# 36. Partition Pruning vs Data Skipping

Les deux mécanismes sont différents.

```text
Partition Pruning
→ élimine des partitions physiques
→ grâce au filtre sur une colonne de partition

Data Skipping
→ élimine des fichiers
→ grâce aux statistiques des fichiers
```

Les deux permettent de réduire le volume de données réellement lu.

---

# 37. Clustering

Une organisation physique adaptée des données peut améliorer les performances de lecture et le Data Skipping.

Dans Databricks moderne, on peut notamment rencontrer des mécanismes de clustering tels que **Liquid Clustering**.

Le principe général à retenir est :

```text
ne pas partitionner mécaniquement toutes les tables
```

Le choix de l'organisation physique doit dépendre :

- du volume ;
- de la cardinalité ;
- des patterns de requêtes ;
- des filtres ;
- de l'évolution des données.

---

# 38. Cache Spark

Un DataFrame coûteux utilisé plusieurs fois peut être mis en cache.

Exemple :

```python
silver_df.cache()

silver_df.count()

silver_df.groupBy(
    "callsign"
).count()

silver_df.filter(
    "altitude > 10000"
).count()
```

Le cache peut éviter de recalculer tout le lineage plusieurs fois.

Mais :

```text
cache()
```

ne doit pas être utilisé systématiquement.

Il consomme des ressources.

Si un DataFrame n'est utilisé qu'une seule fois, le cache peut ne présenter aucun intérêt.

---

# 39. Lazy Evaluation et cache

Comme de nombreuses opérations Spark, `cache()` est lié au modèle d'exécution lazy.

Cette instruction :

```python
df.cache()
```

ne signifie pas nécessairement que toutes les données sont immédiatement calculées.

Une action telle que :

```python
df.count()
```

déclenche le calcul et permet la matérialisation du cache.

---

# 40. Unity Catalog

Unity Catalog est la couche de **gouvernance centralisée** de Databricks.

Il permet notamment :

- le catalogage ;
- l'organisation des objets ;
- la gestion des permissions ;
- la gouvernance ;
- la traçabilité ;
- le lineage.

La hiérarchie principale est :

```text
CATALOG
   ↓
SCHEMA
   ↓
TABLE
```

Exemple pour le projet :

```text
aviation
│
├── bronze
│   └── aircraft_positions
│
├── silver
│   └── aircraft_positions
│
└── gold
    └── airline_statistics
```

Une table pourrait alors être référencée avec :

```sql
SELECT *
FROM aviation.gold.airline_statistics;
```

La notation est :

```text
catalog.schema.table
```

---

# 41. Permissions Unity Catalog

Unity Catalog permet de contrôler les accès.

Par exemple, une équipe BI pourrait avoir accès à :

```text
aviation.gold.airline_statistics
```

sans avoir nécessairement accès aux données Bronze.

Conceptuellement :

```text
Data Engineer
│
├── Bronze → READ / WRITE
├── Silver → READ / WRITE
└── Gold   → READ / WRITE

Data Analyst
│
├── Bronze → pas nécessairement accessible
├── Silver → selon besoin
└── Gold   → READ

Power BI
└── Gold → READ
```

Unity Catalog gère les autorisations.

Il ne constitue pas le moteur de calcul.

---

# 42. Lineage

La gouvernance permet également de suivre les dépendances entre objets.

Exemple :

```text
bronze.aircraft_positions
          ↓
silver.aircraft_positions
          ↓
gold.airline_statistics
          ↓
       Power BI
```

Ce lineage est utile pour comprendre :

- d'où vient une donnée ;
- quelles transformations elle a subies ;
- quelles tables dépendent d'une autre table ;
- quel impact pourrait avoir une modification.

---

# 43. Databricks Compute

Les traitements Spark ont besoin de ressources de calcul.

Conceptuellement :

```text
Notebook / Job
      ↓
Databricks Compute
      ↓
Spark
      ↓
Delta Lake
```

Delta Lake stocke/organise les tables transactionnelles.

Le compute exécute les traitements.

---

# 44. Databricks SQL Warehouse

Pour des workloads SQL et analytiques, Databricks propose des **SQL Warehouses**.

Architecture possible :

```text
Power BI
   ↓
SQL
   ↓
Databricks SQL Warehouse
   ↓
Gold Delta
```

Le SQL Warehouse fournit du **compute SQL**.

Il ne faut pas le confondre avec la notion architecturale générale de Data Warehouse.

---

# 45. Databricks Workflows

Databricks Workflows permet d'orchestrer des traitements Databricks.

Exemple :

```text
Task 1
Bronze ingestion
      ↓
Task 2
Bronze → Silver
      ↓
Task 3
Silver → Gold
      ↓
Task 4
Data Quality
```

Les workflows peuvent notamment gérer :

- les dépendances ;
- les retries ;
- la planification ;
- les paramètres ;
- le monitoring.

---

# 46. Databricks Workflows vs Airflow

Databricks Workflows et Airflow peuvent avoir des rôles différents.

## Databricks Workflows

Très adapté lorsque les traitements sont principalement réalisés dans Databricks :

```text
notebooks
Spark
Delta
SQL
```

## Airflow

Airflow est un orchestrateur généraliste.

Il peut piloter des pipelines impliquant :

```text
API
GCS
Databricks
BigQuery
dbt
autres systèmes
```

Une architecture possible est :

```text
AIRFLOW
   │
   ├── ingestion API
   │
   ├── stockage
   │
   └── déclenche Databricks Job
                    │
                    ↓
             Bronze → Silver
                    ↓
                   Gold
```

Il n'est cependant pas nécessaire d'empiler plusieurs orchestrateurs sans besoin.

Si l'ensemble du pipeline est dans Databricks, Databricks Workflows peut suffire.

---

# 47. Architecture cible de production

Une architecture Databricks/Delta cohérente pour la Toulouse Aviation Data Platform serait :

```text
                  SOURCES
                     │
              API / Kafka / Batch
                     │
                     ▼
             ┌───────────────┐
             │    BRONZE     │
             │  Delta Lake   │
             │ proche source │
             └───────┬───────┘
                     │
             Spark / Databricks
                     │
        typage + DQ + déduplication
                     │
             ┌───────┴────────┐
             ▼                ▼
       ┌──────────┐      ┌──────────┐
       │ REJECTED │      │  SILVER  │
       │Quarantine│      │  Delta   │
       └──────────┘      └────┬─────┘
                              │
                       MERGE / enrichissement
                              │
                              ▼
                        ┌──────────┐
                        │   GOLD   │
                        │  Delta   │
                        │ métier/BI│
                        └────┬─────┘
                             │
                      SQL Warehouse
                             │
                             ▼
                         Power BI
```

Unity Catalog fournit la couche de gouvernance autour de cette architecture.

Databricks Workflows peut orchestrer les traitements Databricks.

Airflow peut rester l'orchestrateur global lorsque le pipeline traverse plusieurs systèmes.

---

# 48. Architecture de stockage

Il est important de distinguer le calcul, la gouvernance et le stockage.

```text
Spark / Databricks Compute
→ calcul

SQL Warehouse
→ compute SQL

Unity Catalog
→ gouvernance / permissions / catalogage / lineage

Delta Lake
→ couche de table transactionnelle

Parquet + _delta_log
→ représentation physique/logique de la table Delta sur le stockage
```

Dans le TP local, les tables Delta sont stockées sur le filesystem WSL :

```text
data/delta/
```

Dans une architecture cloud, les fichiers peuvent résider sur un stockage objet adapté à la plateforme.

---

# 49. Résultats du TP

Le projet a permis de construire une architecture Medallion fonctionnelle :

```text
RAW JSONL
    ↓
BRONZE DELTA
    ↓
TYPAGE
    ↓
DATA QUALITY
    ↓
DEDUPLICATION
    ↓
SILVER DELTA
    ↓
MERGE INCREMENTAL
    ↓
ENRICHISSEMENT
    ↓
AGREGATION
    ↓
GOLD DELTA
```

Résultats principaux :

```text
Bronze
12 lignes

Silver avant déduplication
9 lignes valides

Silver après déduplication
8 lignes

Silver après MERGE incrémental
9 lignes

Gold
6 lignes agrégées par compagnie/code compagnie
```

---

# 50. Fichiers du TP

Les principaux scripts développés sont :

```text
spark/jobs/delta_bronze_demo.py
spark/jobs/delta_time_travel_demo.py
spark/jobs/delta_merge_demo.py
spark/jobs/delta_schema_demo.py
spark/jobs/delta_schema_evolution_demo.py
spark/jobs/delta_silver_demo.py
spark/jobs/delta_silver_merge_demo.py
spark/jobs/delta_gold_demo.py
```

Tables locales :

```text
data/delta/bronze/aircraft_positions/
data/delta/silver/aircraft_positions/
data/delta/gold/airline_statistics/
```

Les données Delta locales ne sont pas destinées à être versionnées dans Git.

Le code et la documentation sont versionnés.

---

# 51. Points d'attention production

Le TP est volontairement simplifié.

Dans une architecture de production, il faudrait notamment considérer :

- stockage cloud ;
- gestion des identités ;
- permissions ;
- secrets ;
- Unity Catalog ;
- stratégie de partitionnement/clustering ;
- stratégie de rétention ;
- monitoring ;
- observabilité ;
- Data Quality ;
- gestion des rejets ;
- reprise sur erreur ;
- idempotence ;
- traitement incrémental ;
- coûts de compute ;
- optimisation des fichiers ;
- CI/CD ;
- orchestration ;
- tests.

---

# 52. Points clés à retenir

## Spark

```text
moteur de calcul distribué
```

## Delta Lake

```text
tables transactionnelles
Parquet + _delta_log
ACID
Time Travel
MERGE
Schema Enforcement
Schema Evolution
```

## Bronze

```text
données proches de la source
traçabilité
```

## Silver

```text
données typées
validées
nettoyées
dédupliquées
```

## Gold

```text
données métier
agrégations
BI
```

## OPTIMIZE

```text
réorganisation / compaction des fichiers
```

## VACUUM

```text
suppression d'anciens fichiers obsolètes
attention au Time Travel
```

## Partition Pruning

```text
évite de lire certaines partitions
```

## Data Skipping

```text
évite de lire certains fichiers grâce aux statistiques
```

## Unity Catalog

```text
gouvernance
permissions
catalogage
lineage
```

## SQL Warehouse

```text
compute SQL / analytics
```

## Databricks Workflows

```text
orchestration des workloads Databricks
```

---

# 53. Questions d'entretien — réponses synthétiques

## Quelle différence entre Spark et Delta Lake ?

Spark est un moteur de calcul distribué.

Delta Lake apporte une couche de table transactionnelle au-dessus de fichiers de données et fournit notamment ACID, Time Travel, MERGE et la gestion du schéma.

---

## Que contient `_delta_log` ?

Le `_delta_log` contient l'historique transactionnel et les métadonnées permettant de reconstruire l'état logique des différentes versions d'une table Delta.

Les données elles-mêmes sont principalement stockées dans les fichiers Parquet.

---

## À quoi sert MERGE ?

MERGE permet notamment de réaliser des UPSERT :

```text
MATCH
→ UPDATE

NO MATCH
→ INSERT
```

Il est particulièrement utile pour les pipelines incrémentaux, les corrections et certains traitements CDC.

---

## Différence entre Atomicity et Isolation ?

Atomicity :

```text
une transaction est entièrement appliquée ou pas du tout
```

Isolation :

```text
les transactions concurrentes ne doivent pas exposer d'états intermédiaires incohérents
```

---

## Pourquoi utiliser try_cast ?

Pour effectuer une conversion tolérante.

Une valeur impossible à convertir devient `NULL`, ce qui permet ensuite à la Data Quality de détecter et traiter la donnée invalide sans nécessairement faire échouer tout le pipeline.

---

## Différence entre Schema Enforcement et Schema Evolution ?

Schema Enforcement :

```text
empêche une écriture incompatible avec le schéma attendu
```

Schema Evolution :

```text
permet une modification contrôlée du schéma
```

---

## Différence entre OPTIMIZE et VACUUM ?

OPTIMIZE :

```text
réorganise / compacte les fichiers
```

VACUUM :

```text
supprime les anciens fichiers devenus obsolètes selon la rétention
```

---

## Pourquoi VACUUM peut-il affecter Time Travel ?

Parce que `_delta_log` ne contient pas une copie complète des données.

Si un ancien fichier Parquet nécessaire à une ancienne version est physiquement supprimé, cette version peut devenir impossible à lire.

---

## Différence entre repartition et partitionBy ?

`repartition()` concerne les partitions Spark utilisées pendant le calcul.

`write.partitionBy()` concerne l'organisation physique des données écrites.

---

## Différence entre repartition et coalesce ?

`repartition()` provoque généralement un shuffle et permet de redistribuer les données, en augmentant ou diminuant le nombre de partitions.

`coalesce()` est surtout utilisé pour réduire le nombre de partitions avec moins de redistribution.

---

## À quoi sert Data Skipping ?

À éviter la lecture de fichiers dont les statistiques montrent qu'ils ne peuvent pas contenir les valeurs recherchées.

---

## À quoi sert Unity Catalog ?

Unity Catalog centralise la gouvernance des données dans Databricks :

- catalogage ;
- permissions ;
- organisation des objets ;
- lineage ;
- traçabilité.

---

## À quoi sert un SQL Warehouse ?

À fournir du compute optimisé pour les workloads SQL et analytiques, par exemple pour exposer une table Gold à Power BI.

---

## Databricks Workflows remplace-t-il forcément Airflow ?

Non.

Databricks Workflows est particulièrement adapté à l'orchestration des traitements Databricks.

Airflow reste pertinent comme orchestrateur global lorsque le pipeline implique plusieurs plateformes ou systèmes.

---

# 54. Validation du module

Le module a permis de mettre en pratique :

- Spark vs Databricks vs Delta Lake ;
- architecture Lakehouse ;
- architecture Medallion ;
- Bronze ;
- Silver ;
- Gold ;
- tables Delta ;
- fichiers Parquet ;
- `_delta_log` ;
- transactions ACID ;
- Time Travel ;
- MERGE / UPSERT ;
- Schema Enforcement ;
- Schema Evolution ;
- Data Quality ;
- Quarantine / Rejected ;
- déduplication ;
- traitements incrémentaux ;
- OPTIMIZE ;
- VACUUM ;
- partitionnement ;
- Partition Pruning ;
- Data Skipping ;
- `repartition()` ;
- `coalesce()` ;
- cache Spark ;
- Unity Catalog ;
- lineage ;
- Databricks Compute ;
- SQL Warehouse ;
- Databricks Workflows ;
- articulation Airflow / Databricks.

**Quiz final : 11/12 — 9,2/10.**

Point de vigilance identifié :

```text
_delta_log
→ historique transactionnel / métadonnées / état des versions
→ ne contient pas toutes les données de la table
```

---

# 55. Conclusion

Ce module a permis de faire évoluer la Toulouse Aviation Data Platform vers une architecture de type Lakehouse.

La chaîne construite est :

```text
Source
  ↓
Bronze Delta
  ↓
Data Quality
  ↓
Silver Delta
  ↓
MERGE incrémental
  ↓
Gold Delta
  ↓
SQL / BI
```

L'utilisation de Delta Lake apporte une gestion transactionnelle et versionnée des données tout en conservant une architecture reposant sur des fichiers.

Cette architecture constitue une base pour une implémentation future sur une plateforme Databricks cloud avec gouvernance Unity Catalog, orchestration et exposition analytique.
