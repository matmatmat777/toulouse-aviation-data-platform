# Module 7 — Apache Airflow

## Toulouse Aviation Data Platform

**Durée du module : ~10 h**  
**Statut : terminé**

---

# 1. Objectifs du module

L'objectif de ce module est de comprendre et mettre en pratique Apache Airflow pour orchestrer les traitements batch de la plateforme Toulouse Aviation Data Platform.

Le pipeline mis en place permet de :

- orchestrer un job PySpark ;
- contrôler les dépendances entre tâches ;
- paramétrer un traitement par date ;
- utiliser les XCom ;
- gérer les retries ;
- contrôler la présence des données RAW ;
- contrôler les données produites ;
- partitionner les traitements par date ;
- rendre les traitements rejouables ;
- garantir l'idempotence d'un traitement ;
- effectuer un replay ciblé ;
- comprendre les notions de catchup et backfill ;
- comprendre les Pools et la gestion du parallélisme ;
- utiliser les logs Airflow pour diagnostiquer les erreurs.

---

# 2. Rôle d'Airflow

Apache Airflow est un orchestrateur de workflows.

Son rôle n'est pas d'effectuer lui-même les transformations massives de données.

Dans notre architecture :

```text
Kafka
  ↓
Consumer
  ↓
RAW
  ↓
Airflow
  ↓
PySpark
  ↓
Processed / Rejected
  ↓
BigQuery
  ↓
dbt
  ↓
Power BI
```

Airflow pilote les différentes étapes.

PySpark réalise les calculs distribués.

Une distinction importante est donc :

```text
Airflow = orchestration

Spark = calcul / transformation
```

Airflow ne remplace pas Spark et Spark ne remplace pas Airflow.

---

# 3. DAG

Un DAG signifie :

**Directed Acyclic Graph**

Il représente un ensemble de tâches ainsi que leurs dépendances.

Exemple :

```text
A
↓
B
↓
C
```

La tâche B ne peut démarrer que lorsque A satisfait les conditions nécessaires.

La tâche C dépend ensuite de B.

Un DAG ne doit pas contenir de boucle cyclique.

---

# 4. Task

Une Task représente une unité de travail dans un DAG.

Exemples dans notre projet :

```text
start
get_processing_date
check_raw_file
run_pyspark
check_processed_data
end
```

Chaque tâche possède :

- un état ;
- des logs ;
- une date d'exécution ;
- un nombre de tentatives ;
- éventuellement des retries ;
- éventuellement une valeur transmise via XCom.

---

# 5. Operators

Un Operator définit le type d'action qu'une tâche Airflow doit effectuer.

Nous avons notamment utilisé :

```python
BashOperator
```

Le BashOperator permet d'exécuter une commande shell.

Dans notre projet, Airflow utilise le BashOperator pour lancer le job PySpark.

Architecture :

```text
Airflow
   ↓
BashOperator
   ↓
Python du virtualenv PySpark
   ↓
process_aircraft_positions.py
   ↓
Spark
```

Airflow orchestre.

Le BashOperator lance la commande.

PySpark réalise le traitement.

---

# 6. TaskFlow API

Airflow permet également de définir les tâches Python avec la TaskFlow API.

Exemple :

```python
@task
def get_processing_date():
    ...
```

Cela permet d'écrire des DAGs plus lisibles et de gérer facilement les dépendances et les XCom.

---

# 7. Dépendances explicites

Une dépendance peut être déclarée avec :

```python
task_a >> task_b
```

Cela signifie :

```text
task_a
   ↓
task_b
```

La tâche B doit attendre la tâche A.

Cela exprime une dépendance d'exécution.

Cela ne signifie pas nécessairement qu'une donnée est transmise entre A et B.

---

# 8. Dépendances implicites avec TaskFlow

Avec la TaskFlow API :

```python
result = task_a()

task_b(result)
```

Airflow comprend automatiquement que `task_b` dépend de `task_a`.

Le résultat de `task_a` est transmis à `task_b` via XCom.

On obtient donc :

```text
task_a
   ↓
XCom
   ↓
task_b
```

Il n'est alors généralement pas nécessaire d'ajouter :

```python
task_a >> task_b
```

La dépendance est implicite.

---

# 9. XCom

XCom signifie :

**Cross Communication**

Les XCom permettent à des tâches Airflow de s'échanger de petites informations.

Exemples adaptés :

```text
processing_date
chemin d'un fichier
nom d'un bucket
identifiant
statut
compteur
```

Dans notre pipeline :

```text
get_processing_date
        ↓
"2026-09-04"
```

et :

```text
check_raw_file
        ↓
/data/raw/year=2026/month=09/day=04/aircraft_positions.jsonl
```

Ces informations sont ensuite utilisées par les tâches suivantes.

---

# 10. Ce qu'il ne faut pas mettre dans un XCom

Un XCom n'est pas destiné à transporter des volumes importants de données.

Il serait notamment incorrect d'essayer d'y placer :

```text
un DataFrame Spark complet
```

Un DataFrame peut contenir des millions ou milliards de lignes.

La bonne approche est :

```text
Airflow
   ↓
transmet le chemin
   ↓
Spark
   ↓
lit directement les données
```

Airflow transporte donc principalement des métadonnées.

---

# 11. Variables et Connections

Les Variables Airflow permettent de stocker de la configuration.

Exemples :

```text
nom d'un bucket
nom d'un dataset
configuration d'environnement
```

Les Connections sont adaptées aux informations de connexion vers des systèmes externes.

Exemples :

```text
GCP
PostgreSQL
AWS
API
```

Les credentials ne doivent pas être écrits directement dans le code du DAG.

---

# 12. Scheduler Airflow

Le Scheduler décide quelles tâches doivent être exécutées et quand elles peuvent être lancées.

Il tient notamment compte :

- du planning ;
- des dépendances ;
- de l'état des tâches ;
- des DAG runs.

Exemple :

```text
DAG planifié à 02:00
        ↓
Scheduler
        ↓
détermine les tâches exécutables
```

---

# 13. Airflow Executor

L'Airflow Executor détermine comment les tâches Airflow sont exécutées.

Il ne faut pas le confondre avec le Scheduler.

Simplification :

```text
Scheduler
→ quoi et quand exécuter

Airflow Executor
→ comment/où exécuter les tasks Airflow
```

---

# 14. Spark Executor

Le Spark Executor est encore une autre notion.

Il appartient à Apache Spark.

Les executors Spark exécutent les tâches de calcul distribuées sur les partitions de données.

Il faut donc distinguer :

```text
Airflow Scheduler
→ orchestration / décision

Airflow Executor
→ exécution des tâches Airflow

Spark Executor
→ calcul distribué Spark
```

---

# 15. Scheduling

Un DAG Airflow peut être exécuté selon un planning.

Exemple cron :

```text
0 2 * * *
```

signifie :

```text
tous les jours à 02:00
```

Dans notre DAG pédagogique, nous avons utilisé :

```python
schedule=None
```

afin de déclencher manuellement les traitements et choisir la date à traiter.

---

# 16. Params

Notre DAG utilise un paramètre :

```text
processing_date
```

Exemple :

```text
2026-09-03
```

Lors d'un déclenchement manuel, Airflow permet donc de choisir la journée à traiter.

Cette date est ensuite utilisée pour déterminer la partition RAW correspondante.

---

# 17. Partitionnement temporel

Les données sont organisées selon :

```text
year=YYYY/
month=MM/
day=DD/
```

Exemple :

```text
data/raw/
└── year=2026/
    └── month=09/
        └── day=03/
            └── aircraft_positions.jsonl
```

Ce partitionnement permet de cibler précisément les données nécessaires à un traitement.

---

# 18. Construction dynamique du chemin RAW

Pour :

```text
processing_date = 2026-09-03
```

Airflow construit :

```text
data/raw/
year=2026/
month=09/
day=03/
aircraft_positions.jsonl
```

La tâche :

```text
check_raw_file
```

vérifie ensuite que ce fichier existe.

Si le fichier est absent, la tâche échoue.

---

# 19. Pourquoi vérifier le RAW avant Spark ?

Il est inutile de lancer un traitement Spark coûteux si les données nécessaires n'existent pas.

Notre pipeline applique donc :

```text
check_raw_file
      ↓
run_pyspark
```

Si le RAW est absent :

```text
check_raw_file ❌
      ↓
run_pyspark non exécuté
```

Cela permet d'échouer rapidement et proprement.

---

# 20. Passage du chemin RAW à PySpark

Le chemin retourné par `check_raw_file` est stocké dans un XCom.

Le BashOperator récupère cette valeur.

Conceptuellement :

```text
check_raw_file
      ↓
XCom
      ↓
BashOperator
      ↓
--input <raw_file>
      ↓
PySpark
```

Exemple de commande exécutée :

```bash
python process_aircraft_positions.py \
  --input /.../data/raw/year=2026/month=09/day=03/aircraft_positions.jsonl \
  --processing-date 2026-09-03
```

Le job PySpark n'est donc plus lié à un fichier RAW codé en dur.

Airflow décide de la partition à traiter.

Spark reçoit cette information et effectue le calcul.

---

# 21. Séparation des responsabilités

Cette architecture respecte une séparation claire :

```text
Airflow
→ décide QUOI traiter et QUAND

PySpark
→ décide COMMENT transformer les données
```

Cela rend le pipeline plus flexible et plus maintenable.

---

# 22. Pipeline PySpark orchestré

Le job PySpark réalise :

```text
Lecture RAW
    ↓
Schema explicite
    ↓
Data Quality
    ↓
VALID / REJECTED
    ↓
Déduplication
    ↓
Enrichissement compagnies
    ↓
Parquet
```

Lors de nos tests :

```text
RAW                    : 7
VALID BEFORE DEDUP     : 4
REJECTED               : 3
DUPLICATES REMOVED     : 1
VALID AFTER DEDUP      : 3
```

Les trois lignes invalides sont envoyées dans la zone rejected.

---

# 23. Contrôle après Spark

Une tâche Spark peut techniquement terminer sans erreur alors que le résultat fonctionnel attendu n'est pas présent.

Il est donc insuffisant de faire uniquement :

```text
run_pyspark ✅
```

Nous avons ajouté :

```text
check_processed_data
```

Le pipeline devient :

```text
check_raw_file
      ↓
run_pyspark
      ↓
check_processed_data
```

Cette dernière tâche vérifie notamment :

- l'existence du répertoire de sortie ;
- la présence de fichiers Parquet.

---

# 24. Succès technique et succès fonctionnel

Il faut distinguer :

```text
succès technique
```

et :

```text
succès fonctionnel / data
```

Un programme qui retourne un code `0` a terminé techniquement correctement.

Cela ne garantit pas nécessairement que :

- les fichiers attendus existent ;
- les données sont présentes ;
- la partition est correcte ;
- les contrôles qualité sont satisfaits.

D'où l'intérêt des contrôles en amont et en aval.

---

# 25. Zone processed

Les données valides sont écrites dans :

```text
data/processed/aircraft_positions/
year=YYYY/
month=MM/
day=DD/
```

Exemple :

```text
data/processed/aircraft_positions/
└── year=2026/
    └── month=09/
        └── day=03/
            ├── _SUCCESS
            └── part-....snappy.parquet
```

---

# 26. Zone rejected

Les données rejetées suivent le même principe :

```text
data/rejected/aircraft_positions/
year=YYYY/
month=MM/
day=DD/
```

Cela permet de conserver les données invalides pour :

- diagnostic ;
- audit ;
- correction ;
- analyse de qualité.

Les données rejetées ne sont donc pas simplement supprimées.

---

# 27. Idempotence

Un pipeline idempotent produit le même état final lorsqu'il est rejoué plusieurs fois avec la même entrée.

Conceptuellement :

```text
même entrée
+
même date
+
même traitement
=
même état final
```

Dans notre projet, Spark écrit avec :

```python
.mode("overwrite")
```

mais l'overwrite est effectué uniquement sur la partition correspondant à la date traitée.

Exemple :

```text
data/processed/aircraft_positions/
year=2026/month=09/day=03/
```

Un rejeu du 03/09 remplace donc la partition du 03/09.

Il ne rajoute pas les mêmes données à la suite.

---

# 28. Pourquoi ne pas overwrite la racine ?

Il serait dangereux de faire un overwrite sur :

```text
data/processed/aircraft_positions/
```

car cela risquerait de supprimer l'historique des autres journées.

Exemple :

```text
03/09
04/09
05/09
```

Un overwrite global pourrait remplacer tout cet historique.

Nous ciblons donc uniquement :

```text
year=2026/month=09/day=03/
```

pour un traitement du 03/09.

---

# 29. Test d'idempotence réalisé

Nous avons exécuté le pipeline une première fois pour :

```text
2026-09-03
```

Puis nous avons relancé exactement la même date.

Le pipeline a de nouveau terminé correctement.

La partition du 03/09 a été réécrite au lieu de dupliquer les données.

Cela valide le comportement recherché :

```text
Run 1 du 03/09
→ état final A

Run 2 du 03/09
→ état final A
```

---

# 30. Retries

Une tâche Airflow peut être configurée avec :

```python
retries=2
```

Cela signifie :

```text
tentative initiale
+
retry 1
+
retry 2
=
3 tentatives maximum
```

Les retries sont utiles pour les erreurs transitoires.

Exemples :

```text
fichier légèrement en retard
API temporairement indisponible
problème réseau ponctuel
service momentanément indisponible
```

Ils ne corrigent pas une erreur permanente dans le code.

---

# 31. Test de retry réalisé

Nous avons volontairement utilisé un fichier RAW inexistant.

La tâche :

```text
check_raw_file
```

a échoué.

Avec :

```python
retries=2
```

Airflow a effectué jusqu'à trois tentatives.

Nous avons également testé le cas où le fichier devient disponible avant un retry.

La tentative suivante a alors réussi.

Cela simule un scénario réaliste de donnée arrivant avec quelques secondes ou minutes de retard.

---

# 32. Catchup

Le paramètre :

```python
catchup=True
```

permet à Airflow de créer les runs planifiés manqués.

Exemple :

```text
DAG quotidien

J1 → Airflow arrêté
J2 → Airflow arrêté
J3 → Airflow arrêté
J4 → redémarrage
```

Avec :

```text
catchup=True
```

Airflow peut créer les runs historiques manqués.

Avec :

```text
catchup=False
```

Airflow ne crée pas automatiquement ces anciens runs planifiés.

Dans notre projet pédagogique :

```python
catchup=False
```

Nous maîtrisons explicitement la date à traiter avec `processing_date`.

---

# 33. Backfill et replay

Un backfill permet de retraiter des périodes historiques.

Un replay ciblé consiste à rejouer une date ou une partition précise.

Grâce au partitionnement, nous pouvons faire :

```text
processing_date = 2026-09-04
```

sans retraiter le 03/09.

C'est particulièrement utile lorsque :

- une donnée arrive en retard ;
- une partition a échoué ;
- une correction doit être appliquée ;
- un traitement historique doit être rejoué.

---

# 34. Test de replay ciblé réalisé

Nous avons simulé le scénario suivant :

```text
03/09 → données présentes → succès

04/09 → données absentes → échec

04/09 → données arrivent plus tard

04/09 → replay ciblé → succès
```

Lors de la première tentative du 04/09 :

```text
check_raw_file ❌
```

Spark n'a pas été exécuté.

Nous avons ensuite créé la partition RAW :

```text
data/raw/year=2026/month=09/day=04/
```

Puis relancé :

```text
processing_date = 2026-09-04
```

Le pipeline a terminé avec succès.

Les nouvelles sorties ont été créées dans :

```text
data/processed/aircraft_positions/
year=2026/month=09/day=04/
```

et :

```text
data/rejected/aircraft_positions/
year=2026/month=09/day=04/
```

La partition du 03/09 n'a pas eu besoin d'être retraitée.

---

# 35. Sensors

Un Sensor Airflow permet d'attendre qu'une condition soit satisfaite.

Exemples :

```text
attendre un fichier
attendre une partition
attendre une date
attendre la fin d'un traitement externe
```

Dans notre exercice, nous avons utilisé une tâche Python `check_raw_file`.

Dans une architecture Airflow plus poussée, un Sensor peut être utilisé pour attendre l'arrivée d'une ressource.

---

# 36. Trigger Rules

Par défaut, une tâche utilise généralement une logique de type :

```text
all_success
```

Elle démarre lorsque toutes ses dépendances amont nécessaires ont réussi.

D'autres règles existent, notamment :

```text
one_failed
all_done
```

Exemple :

```text
pipeline
   ├── traitement
   └── alerte
```

Une tâche d'alerte peut être configurée pour s'exécuter lorsqu'une tâche amont échoue.

---

# 37. Parallélisme

Airflow peut exécuter plusieurs tâches en parallèle lorsque leurs dépendances le permettent.

Exemple :

```text
      A
     / \
    B   C
     \ /
      D
```

Si :

```text
B = 5 minutes
C = 8 minutes
```

B et C peuvent s'exécuter simultanément.

D devra attendre la fin des deux.

Il pourra donc commencer après environ 8 minutes.

---

# 38. Pools

Les Pools Airflow permettent de limiter l'utilisation d'une ressource partagée.

Exemple :

```text
cluster Spark
capacité maximale = 2 jobs simultanés
```

On peut créer :

```text
spark_pool
slots = 2
```

Si trois jobs Spark arrivent :

```text
Job A → exécution
Job B → exécution
Job C → attente
```

Lorsque A ou B termine, un slot devient disponible et C peut démarrer.

À retenir :

```text
Parallélisme
→ permet plusieurs exécutions simultanées

Pool
→ limite le nombre d'exécutions simultanées
```

Les Pools permettent donc de protéger les ressources.

---

# 39. Logs Airflow

Chaque Task possède ses propres logs.

Ils permettent notamment de connaître :

- la commande exécutée ;
- les paramètres ;
- le nombre de tentatives ;
- les exceptions ;
- les logs Spark ;
- le code retour ;
- l'état final de la tâche.

Nous avons utilisé les logs pour vérifier que le BashOperator exécutait réellement :

```text
process_aircraft_positions.py
--input <partition RAW>
--processing-date <date>
```

---

# 40. Logs Spark dans Airflow

Comme le BashOperator lance le processus PySpark, les logs du job Spark sont visibles depuis Airflow.

Nous avons pu observer :

```text
RAW : 7
VALID BEFORE DEDUP : 4
REJECTED : 3
DUPLICATES REMOVED : 1
VALID AFTER DEDUP : 3
```

Nous avons également retrouvé dans le plan physique Spark :

```text
BroadcastHashJoin
```

pour l'enrichissement avec la petite table de référence des compagnies.

Et :

```text
Exchange hashpartitioning(...)
```

pour la redistribution nécessaire à la déduplication par Window.

Cela relie directement les notions apprises dans le module PySpark au pipeline orchestré par Airflow.

---

# 41. Monitoring et SLA / deadline

Un pipeline peut terminer avec succès tout en étant trop tard.

Exemple :

```text
heure attendue : avant 03:00
pipeline terminé : 04:30
```

Techniquement :

```text
SUCCESS
```

Fonctionnellement :

```text
incident de fraîcheur / délai
```

Il faut donc surveiller non seulement les erreurs techniques mais aussi les délais de mise à disposition des données.

---

# 42. Architecture finale du DAG

Le DAG construit pendant ce module est :

```text
start
  ↓
get_processing_date
  ↓
check_raw_file
  ↓
run_pyspark
  ↓
check_processed_data
  ↓
end
```

Le flux de données et métadonnées est :

```text
processing_date
       ↓
get_processing_date
       ↓
XCom
       ↓
check_raw_file
       ↓
chemin RAW
       ↓
XCom
       ↓
BashOperator
       ↓
PySpark
       ↓
DQ
       ↓
Déduplication
       ↓
Enrichissement
       ↓
Processed / Rejected
       ↓
check_processed_data
       ↓
end
```

---

# 43. Exemple de partition complète

Pour :

```text
processing_date = 2026-09-04
```

entrée :

```text
data/raw/
└── year=2026/
    └── month=09/
        └── day=04/
            └── aircraft_positions.jsonl
```

sortie valide :

```text
data/processed/aircraft_positions/
└── year=2026/
    └── month=09/
        └── day=04/
            ├── _SUCCESS
            └── part-....snappy.parquet
```

sortie rejetée :

```text
data/rejected/aircraft_positions/
└── year=2026/
    └── month=09/
        └── day=04/
            ├── _SUCCESS
            └── part-....snappy.parquet
```

---

# 44. Environnement local

Airflow est installé dans un environnement Python séparé :

```text
/home/matde/.venvs/airflow
```

PySpark possède son propre environnement :

```text
/home/matde/.venvs/toulouse-aviation
```

Cela permet de séparer les dépendances.

Airflow lance explicitement le Python du virtualenv PySpark.

Cette séparation évite d'installer inutilement PySpark dans l'environnement Airflow.

---

# 45. Airflow sous WSL

Le développement est réalisé sous WSL Ubuntu.

Le projet se trouve dans :

```text
/home/matde/projects/toulouse-aviation-data-platform
```

Le dossier DAG du projet est :

```text
airflow/dags/
```

Il est relié au dossier DAG Airflow local.

Cela permet de conserver les DAGs directement dans le repository Git du projet.

---

# 46. Fichier principal du DAG

Le DAG se trouve dans :

```text
airflow/dags/aviation_pipeline.py
```

Le job Spark orchestré se trouve dans :

```text
spark/jobs/process_aircraft_positions.py
```

---

# 47. Gestion des erreurs

Le principe général retenu est :

```text
Erreur temporaire
→ retry

Erreur permanente
→ échec

Donnée manquante
→ ne pas lancer le traitement inutilement

Donnée arrivée plus tard
→ replay ciblé

Sortie absente
→ faire échouer le contrôle aval
```

L'objectif n'est pas de rendre artificiellement tous les traitements verts.

L'objectif est de détecter correctement les anomalies et de permettre une reprise contrôlée.

---

# 48. RAW immuable

La zone RAW ne doit pas être modifiée par le traitement Spark.

Le principe est :

```text
RAW
→ lecture uniquement
```

Puis :

```text
RAW
   ↓
Spark
   ├── processed
   └── rejected
```

En cas de problème, le RAW permet de rejouer les transformations.

---

# 49. Pourquoi le partitionnement est important

Le partitionnement temporel apporte plusieurs avantages :

- traitement ciblé ;
- replay ciblé ;
- backfill ;
- réduction du volume lu ;
- organisation de l'historique ;
- idempotence plus simple ;
- diagnostic plus facile.

Au lieu de retraiter :

```text
tout l'historique
```

on peut traiter :

```text
une journée précise
```

---

# 50. Exemple de réponse en entretien — Airflow

Une présentation synthétique du travail réalisé peut être :

> J'ai construit un DAG Airflow paramétré par date pour orchestrer un pipeline PySpark. Le DAG vérifie la présence de la partition RAW, transmet son chemin au job Spark via XCom et BashOperator, puis contrôle la présence des sorties Parquet. Les sorties processed et rejected sont partitionnées par date. Les traitements sont rejouables et l'écriture en overwrite est limitée à la partition cible afin de garantir l'idempotence. J'ai également testé les retries, les erreurs liées à une donnée manquante et le replay ciblé d'une partition arrivée en retard.

---

# 51. Questions d'entretien révisées

## Quelle différence entre Airflow et Spark ?

Airflow orchestre les traitements.

Spark réalise les calculs distribués.

---

## À quoi sert un XCom ?

À transmettre de petites informations entre tâches Airflow.

Exemples :

```text
date
path
ID
statut
compteur
```

Il ne doit pas servir à transporter un DataFrame massif.

---

## Que signifie `retries=2` ?

Une tentative initiale plus deux nouvelles tentatives.

Donc :

```text
3 tentatives maximum
```

---

## À quoi sert `catchup=True` ?

À permettre la création des runs planifiés historiques qui ont été manqués.

---

## Pourquoi vérifier les données avant et après Spark ?

Avant :

```text
vérifier que l'entrée nécessaire existe
```

Après :

```text
vérifier que le traitement a réellement produit la sortie attendue
```

Un succès technique n'est pas nécessairement un succès fonctionnel.

---

## Qu'est-ce que l'idempotence ?

La capacité d'un traitement à être rejoué avec les mêmes entrées sans modifier l'état final attendu ni créer de doublons ou d'effets de bord indésirables.

---

## Pourquoi overwrite uniquement une partition ?

Pour permettre le rejeu d'une journée sans supprimer l'historique des autres journées.

---

## À quoi sert un Pool Airflow ?

À limiter le nombre de tâches utilisant simultanément une ressource contrainte.

Exemple :

```text
cluster Spark limité à 2 jobs
→ Pool de 2 slots
```

---

## Différence entre `A >> B` et `B(A())` ?

```text
A >> B
```

déclare explicitement une dépendance.

Avec TaskFlow :

```python
result = A()
B(result)
```

la transmission du résultat crée implicitement la dépendance et utilise XCom.

---

# 52. Points à consolider

Les notions à revoir rapidement lors d'une future révision sont :

```text
Airflow Scheduler
vs
Airflow Executor
vs
Spark Executor
```

et :

```text
parallélisme
vs
Airflow Pool
```

Résumé :

```text
Scheduler
→ décide quoi/quand

Airflow Executor
→ détermine comment les tasks Airflow sont exécutées

Spark Executor
→ exécute les calculs Spark

Parallélisme
→ plusieurs tâches simultanées

Pool
→ limite le nombre de tâches simultanées sur une ressource
```

---

# 53. Compétences acquises

À la fin du module, les compétences mises en pratique sont :

- création d'un DAG Airflow ;
- TaskFlow API ;
- BashOperator ;
- dépendances entre tâches ;
- XCom ;
- Params ;
- scheduling ;
- retries ;
- catchup ;
- notion de Sensor ;
- Trigger Rules ;
- Pools ;
- logs et debugging ;
- orchestration PySpark ;
- contrôle amont ;
- contrôle aval ;
- partitionnement temporel ;
- idempotence ;
- replay ciblé ;
- gestion d'une arrivée tardive ;
- séparation orchestration / calcul.

---

# 54. Résultat du quiz final

Score approximatif :

```text
8,3 / 10
```

Points particulièrement bien maîtrisés :

```text
XCom
retries
catchup
idempotence
partitionnement
contrôles amont / aval
replay ciblé
dépendances TaskFlow
```

Points à consolider :

```text
Pool vs parallélisme

Scheduler Airflow
vs Airflow Executor
vs Spark Executor
```

---

# 55. Bilan du module

Le module Airflow est terminé.

Durée estimée :

```text
~10 heures
```

Progression globale :

```text
Modules 1 à 6 : 53 h
Module 7       : 10 h
────────────────────
Total          : 63 h / 121 h
```

Soit environ :

```text
52 %
```

du parcours Toulouse Aviation Data Platform.

---

# 56. État actuel de la plateforme

À ce stade, les briques étudiées comprennent :

```text
Architecture Data moderne
        ↓
GCP
        ↓
BigQuery
        ↓
dbt
        ↓
Kafka
        ↓
PySpark
        ↓
Airflow
```

Le projet permet déjà de démontrer plusieurs compétences de Data Engineer :

```text
ingestion
streaming
stockage RAW
transformation distribuée
data quality
déduplication
partitionnement
orchestration
replay
idempotence
tests
monitoring par logs
```

---

# 57. Prochaine étape

Prochain module :

```text
Module 8 — Terraform
Durée prévue : 12 h
```

Objectif :

```text
Infrastructure as Code
```

Nous utiliserons Terraform pour décrire et gérer l'infrastructure de la plateforme de manière reproductible et versionnée.

Une révision ciblée de Kafka et PySpark sera également effectuée après cette prochaine phase afin de consolider les notions les moins automatiques.

---

# Module 7 — TERMINÉ

**Apache Airflow : ~10 h**

Le pipeline Airflow/PySpark est opérationnel localement, paramétré par date, partitionné, contrôlé et rejouable.