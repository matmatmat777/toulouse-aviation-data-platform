# PySpark — Traitement distribué des positions aériennes

## 1. Objectif du module

Ce module met en œuvre **PySpark** pour transformer les données de positions aériennes du projet **Toulouse Aviation Data Platform**.

Le traitement réalise les étapes suivantes :

1. lecture des données RAW au format JSONL ;
2. application d'un schéma explicite ;
3. contrôles de Data Quality ;
4. séparation des données valides et rejetées ;
5. déduplication des positions ;
6. enrichissement avec un référentiel de compagnies aériennes ;
7. écriture des données propres au format Parquet ;
8. conservation des données rejetées dans une quarantine zone ;
9. tests automatisés avec pytest.

---

## 2. Architecture du traitement

```text
JSONL RAW
   |
   v
PySpark
   |
   +--> Data Quality
   |
   +--> VALID ----------------------+
   |                                |
   |                                v
   |                          Déduplication
   |                                |
   |                                v
   |                          Enrichissement
   |                                |
   |                                v
   |                          Parquet CLEAN
   |
   +--> REJECTED
            |
            v
     Parquet QUARANTINE
```

---

## 3. SparkSession

Le job utilise une `SparkSession` locale :

```python
SparkSession.builder \
    .appName("ProcessAircraftPositions") \
    .master("local[2]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()
```

### Explication

- `local[2]` : exécution locale avec deux threads ;
- `spark.sql.shuffle.partitions = 2` : nombre de partitions réduit pour notre petit dataset local ;
- `UTC` : standardisation des timestamps du pipeline.

En production, ces paramètres seraient adaptés à la taille du cluster et au volume des données.

---

## 4. Driver et Executors

### Driver

Le **Driver** pilote l'application Spark.

Il :

- interprète le code ;
- construit le plan d'exécution ;
- coordonne le job ;
- distribue les tâches.

Le Driver ne récupère pas automatiquement toutes les données.

### Executors

Les **Executors** exécutent les tâches sur les différentes partitions des données.

Ils effectuent concrètement les calculs demandés par le Driver.

Architecture simplifiée :

```text
                  DRIVER
                    |
          construit / coordonne
                    |
          +---------+---------+
          |         |         |
          v         v         v
      EXECUTOR   EXECUTOR   EXECUTOR
          |         |         |
      partition  partition  partition
```

---

## 5. Transformations, Actions et Lazy Evaluation

Spark utilise le **lazy evaluation**.

Les transformations construisent progressivement un plan de calcul sans nécessairement exécuter immédiatement les opérations.

Exemples de transformations :

```python
df.filter(...)
df.select(...)
df.withColumn(...)
df.join(...)
df.groupBy(...)
```

Une **Action** déclenche réellement l'exécution du traitement nécessaire.

Exemples :

```python
df.count()
df.show()
df.collect()
df.write.parquet(...)
```

Cela permet à Spark d'optimiser le plan d'exécution avant de lancer les calculs.

---

## 6. Schéma explicite

Les données sont lues avec un schéma défini avec `StructType`.

Exemple :

```python
AIRCRAFT_SCHEMA = StructType([
    StructField("icao24", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("altitude", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
])
```

### Pourquoi utiliser un schéma explicite ?

Cela permet :

- de contrôler les types attendus ;
- d'éviter de dépendre de l'inférence automatique ;
- de détecter plus facilement les données incorrectes ;
- d'obtenir un comportement stable entre les exécutions.

Par exemple, une altitude contenant :

```text
PAS_UN_NOMBRE
```

ne correspond pas au `DoubleType`.

Elle peut alors devenir :

```text
NULL
```

et être détectée par nos règles de Data Quality.

---

## 7. Data Quality

Les données sont contrôlées avant les transformations métier.

Les règles implémentées vérifient notamment :

- `icao24` obligatoire ;
- `timestamp` obligatoire ;
- `ingestion_timestamp` obligatoire ;
- latitude obligatoire ;
- latitude comprise entre -90 et 90 ;
- longitude obligatoire ;
- longitude comprise entre -180 et 180 ;
- altitude obligatoire ;
- altitude supérieure ou égale à 0.

Une colonne :

```text
rejection_reason
```

est ajoutée.

Exemples de motifs :

```text
MISSING_ICAO24
INVALID_OR_MISSING_TIMESTAMP
INVALID_OR_MISSING_INGESTION_TIMESTAMP
INVALID_OR_MISSING_LATITUDE
LATITUDE_OUT_OF_RANGE
INVALID_OR_MISSING_LONGITUDE
LONGITUDE_OUT_OF_RANGE
INVALID_OR_MISSING_ALTITUDE
ALTITUDE_BELOW_ZERO
```

Une ligne valide possède :

```text
rejection_reason = NULL
```

---

## 8. Séparation VALID / REJECTED

Après les contrôles qualité, deux DataFrames sont créés :

```python
df_valid
df_rejected
```

Les lignes valides poursuivent le pipeline.

Les données incorrectes ne sont pas simplement supprimées.

Elles sont conservées avec leur `rejection_reason`.

```text
RAW
 |
 v
Data Quality
 |
 +---- VALID ------> pipeline
 |
 +---- REJECTED ---> quarantine
```

Cette stratégie permet :

- la traçabilité ;
- le debugging ;
- l'audit ;
- l'analyse des anomalies ;
- un éventuel retraitement.

---

## 9. Déduplication

La clé métier utilisée pour identifier une position est :

```text
(icao24, timestamp)
```

Deux événements ayant le même avion et le même timestamp métier sont considérés comme représentant la même position logique.

Exemple :

```text
icao24   timestamp   altitude   ingestion_timestamp
39abcd   13:00:00    10000      13:00:05
39abcd   13:00:00    10100      13:00:08
```

La règle métier consiste à conserver l'événement ayant le :

```text
ingestion_timestamp
```

le plus récent.

---

## 10. Window Function et row_number()

La déduplication utilise une Window :

```python
dedup_window = (
    Window
    .partitionBy(
        "icao24",
        "timestamp"
    )
    .orderBy(
        col("ingestion_timestamp").desc()
    )
)
```

Puis :

```python
row_number().over(dedup_window)
```

Chaque ligne reçoit un numéro à l'intérieur de sa clé métier.

```text
icao24   timestamp   altitude   ingestion_timestamp   row_number
39abcd   13:00:00    10100      13:00:08              1
39abcd   13:00:00    10000      13:00:05              2
```

On conserve :

```text
row_number = 1
```

Résultat :

```text
39abcd   13:00:00   10100   13:00:08
```

---

## 11. Shuffle

Un **shuffle** correspond à une redistribution des données entre les partitions/executors.

Certaines opérations peuvent provoquer un shuffle :

```python
groupBy()
join()
repartition()
Window.partitionBy()
```

Par exemple :

```python
df.groupBy("airline_code").count()
```

Spark doit regrouper les lignes ayant le même `airline_code`.

Les données peuvent donc devoir passer d'une partition à une autre.

Le shuffle peut être coûteux en :

- réseau ;
- disque ;
- CPU ;
- mémoire.

Il faut donc éviter les shuffles inutiles sur les très gros volumes.

---

## 12. Référentiel des compagnies aériennes

Le pipeline possède un petit référentiel :

```csv
airline_code,airline_name
AFR,Air France
RYR,Ryanair
EZY,easyJet
BAW,British Airways
DLH,Lufthansa
```

Le callsign permet d'extraire le code compagnie.

Exemples :

```text
AFR123 -> AFR
RYR456 -> RYR
```

En PySpark :

```python
substring(
    col("callsign"),
    1,
    3
)
```

Une colonne `airline_code` est ainsi ajoutée.

---

## 13. Broadcast Join

Le dataset des positions peut devenir très volumineux alors que le référentiel des compagnies reste très petit.

Exemple théorique :

```text
Positions : 500 000 000 lignes
Airlines  : 300 lignes
```

Il serait inutile de redistribuer massivement les positions pour joindre seulement quelques centaines de compagnies.

On utilise donc :

```python
broadcast(df_airlines)
```

Exemple :

```python
df_positions.join(
    broadcast(df_airlines),
    on="airline_code",
    how="left"
)
```

La petite table de référence est copiée vers les executors.

Cela permet d'éviter un shuffle massif de la grande table de positions.

Dans le plan physique Spark, on peut vérifier la présence de :

```text
BroadcastHashJoin
```

---

## 14. Pourquoi un LEFT JOIN ?

Le pipeline utilise :

```text
LEFT JOIN
```

afin de conserver toutes les positions.

Si une compagnie n'existe pas dans notre référentiel :

```text
XYZ999
```

on pourra obtenir :

```text
airline_code = XYZ
airline_name = NULL
```

mais la position aérienne reste présente.

Le type de JOIN répond donc à une question métier différente du broadcast :

- `LEFT JOIN` → quelles lignes veut-on conserver ?
- `broadcast()` → comment optimiser physiquement le JOIN ?

---

## 15. Format Parquet

Les données nettoyées et enrichies sont écrites au format **Parquet**.

Parquet apporte notamment :

- stockage colonnaire ;
- conservation des types ;
- compression efficace ;
- meilleure efficacité analytique ;
- column pruning ;
- predicate pushdown.

Le JSONL reste utile pour les données RAW car il est simple, lisible et adapté à la conservation des événements sources.

Parquet est plus adapté aux traitements analytiques.

---

## 16. Column Pruning

Supposons qu'un dataset possède 100 colonnes.

La requête demande seulement :

```python
df.select(
    "icao24",
    "altitude"
)
```

Grâce au format colonnaire Parquet, Spark peut ne lire que les colonnes nécessaires.

Cette optimisation est appelée :

```text
Column Pruning
```

Cela réduit la quantité de données lues.

---

## 17. Predicate Pushdown

Exemple :

```python
df.filter(
    col("altitude") > 10000
)
```

Avec Parquet, Spark peut exploiter les métadonnées/statistiques disponibles pour éviter de lire certains blocs qui ne peuvent pas contenir de valeurs correspondant au filtre.

Cette optimisation est appelée :

```text
Predicate Pushdown
```

---

## 18. Cache

Spark peut recalculer la lineage d'un DataFrame lorsqu'une nouvelle action est déclenchée.

Si un DataFrame coûteux est réutilisé plusieurs fois, on peut utiliser :

```python
df.cache()
```

Le résultat calculé peut alors être réutilisé sans refaire systématiquement tout le calcul amont.

Le cache est intéressant lorsqu'un DataFrame :

- est coûteux à calculer ;
- est réutilisé plusieurs fois.

Il ne faut pas mettre `cache()` partout.

Le cache consomme de la mémoire sur les executors et peut devenir contre-productif.

---

## 19. collect()

La commande :

```python
df.collect()
```

ramène toutes les lignes depuis les executors vers la mémoire du Driver.

Sur un très gros dataset :

```text
500 millions de lignes
```

cela peut saturer la RAM du Driver et provoquer :

```text
OutOfMemoryError
```

Pour inspecter quelques lignes, on préfère :

```python
df.show(20)
```

ou :

```python
df.limit(20).collect()
```

---

## 20. repartition() vs coalesce()

### coalesce()

`coalesce()` est principalement utilisé pour **réduire** le nombre de partitions.

Exemple :

```python
df.coalesce(10)
```

Pour passer par exemple de :

```text
200 partitions
```

à :

```text
10 partitions
```

`coalesce()` peut éviter un shuffle complet.

### repartition()

`repartition()` redistribue les données entre les partitions.

Exemple :

```python
df.repartition(100)
```

Cette opération provoque généralement un shuffle.

Elle est utile notamment pour :

- augmenter le nombre de partitions ;
- redistribuer les données ;
- améliorer l'équilibrage entre partitions.

### Règle simple à retenir

```text
coalesce    -> réduire
repartition -> redistribuer
```

Si les partitions sont très déséquilibrées, `repartition()` peut néanmoins être préférable malgré le coût du shuffle.

---

## 21. Data Skew

Le **data skew** apparaît lorsque certaines clés possèdent beaucoup plus de données que les autres.

Exemple :

```text
AFR = 300 millions
RYR = 80 millions
EZY = 50 millions
autres = beaucoup moins
```

Lors d'un :

```python
df.groupBy("airline_code").count()
```

Spark effectue un shuffle.

Une partition liée à une clé très fréquente peut alors recevoir énormément plus de données que les autres.

Conséquences :

- un executor travaille beaucoup plus longtemps ;
- d'autres executors terminent rapidement et attendent ;
- le job attend la tâche la plus lente ;
- augmentation de la pression mémoire ;
- dégradation des performances.

---

## 22. Quarantine Zone

Les données rejetées sont écrites dans :

```text
data/rejected/aircraft_positions/
```

Elles conservent notamment :

```text
rejection_reason
```

Cette stratégie permet de savoir :

- quelle donnée a été rejetée ;
- quand elle a été ingérée ;
- pourquoi elle a été rejetée ;
- si elle peut être retraitée ultérieurement.

Cela améliore :

- la traçabilité ;
- l'observabilité ;
- le debugging ;
- l'audit ;
- le retraitement.

---

## 23. Refactoring du job

Le pipeline a été découpé en fonctions :

```python
add_quality_checks()
split_valid_rejected()
deduplicate_positions()
enrich_positions()
```

Cela évite d'avoir toute la logique métier dans un seul bloc.

Le découpage rend le code :

- plus lisible ;
- plus maintenable ;
- réutilisable ;
- testable.

L'exécution principale est placée dans :

```python
def main():
    ...
```

avec :

```python
if __name__ == "__main__":
    main()
```

---

## 24. Tests automatisés PySpark

Les transformations principales sont testées avec **pytest**.

Les tests se trouvent dans :

```text
spark/tests/test_process_aircraft_positions.py
```

Trois tests sont actuellement implémentés.

### Test 1 — Data Quality

Une altitude négative :

```text
altitude = -500
```

doit produire :

```text
ALTITUDE_BELOW_ZERO
```

### Test 2 — Déduplication

Pour :

```text
10000 m -> ingestion 13:00:05
10100 m -> ingestion 13:00:08
```

le pipeline doit conserver :

```text
10100 m -> ingestion 13:00:08
```

### Test 3 — Enrichissement

Le test vérifie :

```text
AFR123
   |
   v
AFR
   |
   v
Air France
```

---

## 25. Exécution des tests

Depuis la racine du projet :

```bash
python -m pytest -v spark/tests/
```

Résultat obtenu :

```text
3 passed
```

Les tests valident automatiquement :

- les règles de Data Quality ;
- la règle de déduplication ;
- l'enrichissement avec le référentiel.

---

## 26. Exécution du pipeline

Depuis la racine :

```text
~/projects/toulouse-aviation-data-platform
```

activation de l'environnement Python :

```bash
source /home/matde/.venvs/toulouse-aviation/bin/activate
```

Puis :

```bash
python spark/jobs/process_aircraft_positions.py
```

---

## 27. Sorties du pipeline

### Données propres

```text
data/processed/aircraft_positions/
```

Format :

```text
Parquet
```

### Données rejetées

```text
data/rejected/aircraft_positions/
```

Format :

```text
Parquet
```

Les répertoires générés par Spark peuvent contenir plusieurs fichiers :

```text
part-*.snappy.parquet
_SUCCESS
```

ainsi que certains fichiers techniques locaux.

---

## 28. Résultat du dataset d'exercice

Le dataset RAW contient :

```text
7 lignes
```

Résultat du traitement :

```text
RAW                  : 7
VALID AVANT DEDUP    : 4
REJECTED             : 3
DUPLICATES SUPPRIMES : 1
VALID APRES DEDUP    : 3
```

Trois anomalies sont volontairement présentes :

```text
bad001 -> altitude négative
bad002 -> altitude NULL
bad003 -> altitude non numérique
```

La valeur non numérique devient incompatible avec le schéma `DoubleType` et est traitée comme altitude invalide/manquante.

---

## 29. Structure actuelle de la brique Spark

```text
spark/
├── __init__.py
├── jobs/
│   ├── __init__.py
│   ├── explore_aircraft_positions.py
│   └── process_aircraft_positions.py
│
└── tests/
    ├── __init__.py
    └── test_process_aircraft_positions.py
```

Documentation :

```text
docs/
└── spark/
    └── pyspark-fundamentals.md
```

Données :

```text
data/
├── raw/
│   └── aircraft_positions.jsonl
│
├── reference/
│   └── airlines.csv
│
├── processed/
│   └── aircraft_positions/
│
└── rejected/
    └── aircraft_positions/
```

---

## 30. Compétences démontrées

Ce module démontre l'utilisation de :

- PySpark ;
- SparkSession ;
- DataFrames ;
- schémas explicites ;
- transformations et actions ;
- lazy evaluation ;
- Data Quality ;
- Window Functions ;
- `row_number()` ;
- déduplication ;
- shuffle ;
- broadcast join ;
- LEFT JOIN ;
- enrichissement ;
- Parquet ;
- column pruning ;
- predicate pushdown ;
- cache ;
- partitionnement ;
- `coalesce()` ;
- `repartition()` ;
- data skew ;
- quarantine zone ;
- pytest ;
- tests automatisés de transformations Data.

---

## 31. Points d'attention pour une version production

Le pipeline actuel fonctionne localement.

Une version cloud devra notamment ajouter :

- lecture des données RAW depuis GCS ;
- écriture des données traitées dans GCS ;
- Service Account Spark dédié ;
- IAM avec principe du moindre privilège ;
- gestion robuste des métadonnées d'ingestion ;
- observabilité ;
- métriques de Data Quality ;
- stratégie de partitionnement adaptée au volume ;
- gestion du problème des petits fichiers ;
- orchestration ;
- retries ;
- alerting.

L'orchestration sera ajoutée avec **Apache Airflow** dans le module suivant.

---

## 32. Questions d'entretien à retenir

### Qu'est-ce que le lazy evaluation ?

Les transformations construisent un plan de calcul. Une action déclenche réellement l'exécution.

### Pourquoi un shuffle est-il coûteux ?

Parce que Spark doit redistribuer les données entre partitions/executors, ce qui utilise réseau, disque, CPU et mémoire.

### Pourquoi utiliser un broadcast join ?

Lorsqu'une table est très petite par rapport à l'autre, elle peut être copiée sur les executors afin d'éviter de shuffler massivement la grande table.

### Pourquoi utiliser Parquet ?

Parce qu'il s'agit d'un format colonnaire efficace pour l'analytique, avec compression, types, column pruning et predicate pushdown.

### Pourquoi éviter collect() sur un gros dataset ?

Parce que toutes les données sont ramenées dans la mémoire du Driver, avec un risque d'OutOfMemory.

### Quand utiliser cache() ?

Lorsqu'un DataFrame coûteux à calculer est réutilisé plusieurs fois.

### coalesce ou repartition ?

```text
coalesce    -> principalement réduire le nombre de partitions
repartition -> redistribuer les données avec shuffle
```

### Qu'est-ce que le data skew ?

Une distribution déséquilibrée des données entre partitions, conduisant certains executors à travailler beaucoup plus que les autres.

### Pourquoi conserver les données rejetées ?

Pour assurer la traçabilité, comprendre les anomalies et permettre un éventuel retraitement.

---

## 33. État du module

**Module 6 — PySpark : terminé**

Durée prévue :

```text
8 heures
```

Tests :

```text
3 passed
```

Prochaine étape :

```text
Module 7 — Apache Airflow
```

Objectif : orchestrer progressivement les différentes briques de la Toulouse Aviation Data Platform.