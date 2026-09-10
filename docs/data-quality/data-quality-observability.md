# Data Quality & Observability

## Toulouse Aviation Data Platform

Ce document décrit la stratégie de **Data Quality**, **Data Observability**, **Quality Gate**, **logging**, **monitoring**, **diagnostic d'incident** et **alerting** mise en place dans la Toulouse Aviation Data Platform.

---

# 1. Objectif

Une pipeline Data peut être techniquement fonctionnelle tout en produisant des données inutilisables.

Exemple :

```text
Kafka fonctionne
        ↓
GCS RAW reçoit les données
        ↓
PySpark démarre correctement
        ↓
Le fichier est lisible
        ↓
Mais 42 % des lignes sont invalides
```

D'un point de vue infrastructure, le pipeline fonctionne.

D'un point de vue métier et Data Quality, le lot est mauvais.

L'objectif est donc de pouvoir :

1. contrôler la qualité des données ;
2. calculer des métriques ;
3. détecter les anomalies ;
4. conserver les informations de diagnostic ;
5. empêcher la publication d'un lot critique ;
6. signaler l'incident à Airflow ;
7. diagnostiquer la cause de l'échec ;
8. éventuellement alerter une équipe.

Architecture générale :

```text
RAW
 |
 v
PySpark
 |
 +--> Quality Checks
 |
 +--> Metrics
 |
 +--> Logs
 |
 +--> Persist Metrics
 |
 +--> Global Quality Status
 |
 +--> Quality Gate
          |
          +--> OK / WARNING
          |       |
          |       v
          |    traitement
          |
          +--> FAILED
                  |
                  v
             RuntimeError
                  |
                  v
               Airflow
                  |
                  v
              Diagnostic
                  |
                  v
                Alert
```

---

# 2. Santé technique vs santé des données

Il est important de distinguer deux notions.

## 2.1 Santé technique

La santé technique indique si les composants fonctionnent correctement.

Exemples :

- Spark démarre ;
- Kafka est disponible ;
- GCS est accessible ;
- le fichier RAW existe ;
- le job Python s'exécute ;
- Airflow fonctionne.

## 2.2 Santé des données

La santé des données indique si les données sont conformes aux attentes.

Exemples :

- champs obligatoires présents ;
- coordonnées géographiques valides ;
- absence excessive de doublons ;
- volume cohérent ;
- données suffisamment fraîches.

Un pipeline peut donc être :

```text
TECHNICAL HEALTH = OK
DATA HEALTH      = FAILED
```

Exemple :

```text
RAW      = 1 000 000 lignes
VALID    =   510 000 lignes
REJECTED =   490 000 lignes
```

Spark peut avoir parfaitement exécuté le traitement.

Mais un taux de rejet de 49 % représente une anomalie critique.

---

# 3. Dimensions de Data Quality

Plusieurs dimensions de qualité sont contrôlées.

---

## 3.1 Completeness

La **Completeness** mesure la présence des données obligatoires.

Exemples :

```text
icao24 = NULL
timestamp = NULL
ingestion_timestamp = NULL
```

Ces données ne peuvent pas être correctement exploitées.

Exemples de règles :

```python
col("icao24").isNull()
col("timestamp").isNull()
col("ingestion_timestamp").isNull()
```

---

## 3.2 Validity

La **Validity** vérifie qu'une valeur respecte les règles attendues.

Exemples :

Une latitude doit être comprise entre :

```text
-90 et +90
```

Une longitude doit être comprise entre :

```text
-180 et +180
```

Une altitude ne doit pas être négative dans notre règle métier actuelle.

Exemples d'anomalies :

```text
latitude = 350
longitude = 500
altitude = -500
```

---

## 3.3 Uniqueness

La **Uniqueness** concerne les doublons.

Dans notre pipeline, une position est identifiée notamment par :

```text
icao24
timestamp
```

Plusieurs événements peuvent cependant exister pour cette même clé.

Nous conservons alors celui ayant le :

```text
ingestion_timestamp
```

le plus récent.

PySpark utilise une Window :

```python
Window \
    .partitionBy(
        "icao24",
        "timestamp",
    ) \
    .orderBy(
        col("ingestion_timestamp").desc()
    )
```

puis :

```python
row_number()
```

La première ligne est conservée.

---

## 3.4 Consistency

La **Consistency** vérifie que différentes informations restent cohérentes entre elles ou entre plusieurs systèmes.

Exemples possibles :

```text
airline_code cohérent avec callsign
airport_code présent dans le référentiel
timestamps cohérents
```

Cette dimension pourra être enrichie lors des évolutions futures de la plateforme.

---

## 3.5 Freshness

La **Freshness** mesure la fraîcheur des données.

On cherche à répondre à :

> Depuis combien de temps aucune nouvelle donnée n'a-t-elle été ingérée ?

La formule utilisée est :

```text
reference_time - latest_ingestion_timestamp
```

En PySpark, le timestamp le plus récent est obtenu avec :

```python
spark_max("ingestion_timestamp")
```

Puis :

```python
freshness_delay = (
    reference_time
    - latest_ingestion_timestamp
)

freshness_minutes = (
    freshness_delay.total_seconds()
    / 60
)
```

---

## 3.6 Volume

La dimension **Volume** permet de détecter une chute ou une augmentation anormale du nombre d'événements.

Exemple :

```text
Volume attendu : 1 000 000
Volume reçu    :    20 000
```

Le pipeline fonctionne peut-être techniquement, mais il manque probablement une quantité importante de données.

---

# 4. Contrôles ligne par ligne

Les données RAW sont contrôlées avant transformation.

La fonction principale ajoute une colonne :

```text
rejection_reason
```

Exemples de raisons :

```text
MISSING_ICAO24
MISSING_TIMESTAMP
MISSING_INGESTION_TIMESTAMP
INVALID_OR_MISSING_LATITUDE
INVALID_LATITUDE
INVALID_OR_MISSING_LONGITUDE
INVALID_LONGITUDE
INVALID_OR_MISSING_ALTITUDE
ALTITUDE_BELOW_ZERO
```

Les données sont ensuite séparées en deux DataFrames :

```text
VALID
REJECTED
```

Architecture :

```text
RAW
 |
 v
Quality Checks
 |
 +-------------------+
 |                   |
 v                   v
VALID              REJECTED
```

Les données rejetées constituent une forme de **quarantaine**.

Elles ne sont pas silencieusement supprimées.

---

# 5. Taux de rejet

Le taux de rejet est calculé avec :

```text
rejected_count
---------------- × 100
raw_count
```

Implémentation :

```python
def compute_rejection_rate(
    raw_count,
    rejected_count,
):
    if raw_count <= 0:
        return 0.0

    return (
        rejected_count
        / raw_count
        * 100
    )
```

Exemple réel de notre dataset :

```text
RAW      = 7
REJECTED = 3
```

Donc :

```text
3 / 7 × 100
= 42.86 %
```

---

# 6. Statut du taux de rejet

Les seuils actuels sont :

```text
0 %  → 5 %     OK
>5 % → 20 %    WARNING
>20 %          FAILED
```

Implémentation :

```python
def get_rejection_status(
    rejection_rate,
):
    if rejection_rate <= 5:
        return "OK"

    elif rejection_rate <= 20:
        return "WARNING"

    else:
        return "FAILED"
```

Pour notre dataset :

```text
42.86 %
```

donne :

```text
FAILED
```

---

# 7. Taux de doublons

Le taux de doublons est calculé sur les données valides avant déduplication.

Formule :

```text
duplicates_removed
----------------------- × 100
valid_before_dedup
```

Implémentation :

```python
def compute_duplicate_rate(
    valid_before_dedup,
    duplicates_removed,
):
    if valid_before_dedup <= 0:
        return 0.0

    return (
        duplicates_removed
        / valid_before_dedup
        * 100
    )
```

Dans notre dataset :

```text
VALID BEFORE DEDUP = 4
DUPLICATES REMOVED = 1
```

Donc :

```text
1 / 4 × 100
= 25 %
```

---

# 8. Statut des doublons

Les seuils utilisés sont :

```text
0 %  → 2 %     OK
>2 % → 10 %    WARNING
>10 %          FAILED
```

Implémentation :

```python
def get_duplicate_status(
    duplicate_rate,
):
    if duplicate_rate <= 2:
        return "OK"

    elif duplicate_rate <= 10:
        return "WARNING"

    else:
        return "FAILED"
```

Dans notre exemple :

```text
25 %
```

donne :

```text
FAILED
```

---

# 9. Contrôle de volumétrie

Le volume réellement reçu est simplement le nombre de lignes présentes dans le RAW :

```python
actual_count = df_raw.count()
```

La variation par rapport au volume attendu est :

```text
actual_count - expected_count
-------------------------------- × 100
expected_count
```

Implémentation :

```python
def compute_volume_variation_rate(
    expected_count,
    actual_count,
):
    if (
        expected_count is None
        or expected_count <= 0
    ):
        return 0.0

    return (
        (actual_count - expected_count)
        / expected_count
        * 100
    )
```

---

# 10. Interprétation de la volumétrie

Exemple :

```text
expected = 100
actual   = 75
```

Donc :

```text
(75 - 100) / 100 × 100
= -25 %
```

Cela signifie :

```text
25 % de données en moins que prévu
```

Autre exemple :

```text
expected = 100
actual   = 110
```

Résultat :

```text
+10 %
```

Cela signifie :

```text
10 % de données supplémentaires
```

Le signe est donc utile :

```text
variation négative → moins de données
variation positive → plus de données
```

---

# 11. Statut du volume

Nous utilisons la valeur absolue pour déterminer la gravité :

```python
absolute_variation = abs(
    volume_variation_rate
)
```

Seuils :

```text
variation <= 10 %     OK
variation <= 20 %     WARNING
variation > 20 %      FAILED
```

Implémentation :

```python
def get_volume_status(
    volume_variation_rate,
):
    absolute_variation = abs(
        volume_variation_rate
    )

    if absolute_variation <= 10:
        return "OK"

    elif absolute_variation <= 20:
        return "WARNING"

    else:
        return "FAILED"
```

---

# 12. Origine du volume attendu

Dans notre exercice, le volume attendu peut être fourni au job :

```bash
--expected-count 7
```

Cela permet d'apprendre et de tester le mécanisme.

En production, il serait préférable de calculer automatiquement une baseline.

Exemple :

```text
Historique GCS RAW
        ↓
volume J-1
volume J-7
moyenne 7 jours
même créneau horaire
        ↓
expected_count
```

Puis :

```text
RAW actuel
   ↓
actual_count
   ↓
comparaison
```

Il ne faut pas nécessairement demander au Producer combien PySpark doit avoir reçu.

En effet :

```text
Producer : 500 000 événements
Kafka    : 500 000 événements
GCS RAW  : 300 000 événements
```

Si PySpark lit GCS RAW :

```text
actual_count = 300 000
```

C'est précisément ce qui permet de détecter une perte en amont.

Une observabilité plus avancée pourrait comparer :

```text
Producer count
Kafka count
GCS RAW count
Spark count
VALID count
```

afin de localiser précisément la perte.

---

# 13. Freshness

La freshness utilise :

```text
latest_ingestion_timestamp
```

Exemple :

```text
reference_time              = 13:30
latest_ingestion_timestamp  = 13:05
```

Alors :

```text
freshness ≈ 25 minutes
```

---

# 14. Seuils de Freshness

Les seuils utilisés sont :

```text
0 → 30 minutes       OK
>30 → 120 minutes    WARNING
>120 minutes         FAILED
```

Implémentation :

```python
def get_freshness_status(
    freshness_minutes,
):
    if freshness_minutes is None:
        return "FAILED"

    if freshness_minutes < 0:
        return "FAILED"

    if freshness_minutes <= 30:
        return "OK"

    elif freshness_minutes <= 120:
        return "WARNING"

    else:
        return "FAILED"
```

---

# 15. Timestamp futur

Lors d'un test réel dans Airflow, une anomalie intéressante a été détectée :

```text
FRESHNESS : -95.08 min
```

Le premier algorithme contenait :

```python
if freshness_minutes <= 30:
    return "OK"
```

Mathématiquement :

```text
-95 <= 30
```

est vrai.

Le pipeline aurait donc considéré un timestamp apparemment situé dans le futur comme parfaitement frais.

Cette règle a été corrigée :

```python
if freshness_minutes < 0:
    return "FAILED"
```

Une freshness négative peut indiquer :

- incohérence de timezone ;
- mauvaise heure de référence ;
- horloges désynchronisées ;
- timestamp source incorrect ;
- événement daté dans le futur.

C'est un exemple concret de l'intérêt de l'observabilité : une exécution réelle a révélé un cas limite qui n'avait pas été anticipé initialement.

---

# 16. Tests reproductibles de Freshness

Les données de démonstration sont historiques.

Utiliser directement :

```python
datetime.now()
```

rendrait les tests dépendants du moment où ils sont exécutés.

Pour obtenir des tests déterministes, une heure de référence peut être fournie :

```bash
--reference-time "2026-09-03T13:30:00"
```

Ainsi :

```text
mêmes données
+
même reference_time
=
même résultat
```

Un test automatisé ne doit pas devenir rouge simplement parce que l'horloge réelle a avancé.

---

# 17. Statut global

Les différents contrôles produisent chacun un statut :

```text
rejection_status
duplicate_status
volume_status
freshness_status
```

Exemple :

```text
Rejection  = FAILED
Duplicates = FAILED
Volume     = OK
Freshness  = OK
```

La priorité utilisée est :

```text
FAILED > WARNING > OK
```

Implémentation :

```python
def get_global_quality_status(
    statuses,
):
    if "FAILED" in statuses:
        return "FAILED"

    elif "WARNING" in statuses:
        return "WARNING"

    else:
        return "OK"
```

Dans l'exemple :

```text
GLOBAL STATUS = FAILED
```

---

# 18. Quality Gate

Détecter une anomalie ne suffit pas.

Le pipeline doit être capable d'empêcher la publication d'un lot dont la qualité est critique.

Le **Quality Gate** transforme le statut de Data Quality en comportement technique.

Implémentation :

```python
def enforce_quality_gate(
    global_status,
):
    if global_status == "FAILED":
        raise RuntimeError(
            "Data Quality Gate FAILED: "
            "critical data quality threshold exceeded"
        )
```

Architecture :

```text
Data Quality
     ↓
global_status
     ↓
FAILED ?
     |
     +-- NON --> pipeline continue
     |
     +-- OUI
          ↓
    RuntimeError
          ↓
    exit code != 0
```

---

# 19. Pourquoi RuntimeError ?

Un simple log :

```python
logger.error(
    "Quality Gate FAILED"
)
```

ne fait pas échouer le processus.

Il écrit seulement un message.

Le programme peut ensuite terminer avec :

```text
exit code = 0
```

Airflow pourrait alors considérer la tâche comme réussie.

Avec :

```python
raise RuntimeError(
    "Quality Gate FAILED"
)
```

l'exception non gérée provoque un code de sortie non nul.

Airflow peut donc détecter l'échec.

On distingue :

```text
logger.error()
      ↓
OBSERVABILITÉ
"expliquer ce qui s'est passé"

raise RuntimeError()
      ↓
QUALITY GATE
"faire réellement échouer le traitement"
```

Les deux sont complémentaires.

---

# 20. WARNING vs FAILED

Un `WARNING` ne doit pas automatiquement bloquer le pipeline.

Exemple :

```text
Rejection  = OK
Duplicates = WARNING
Volume     = OK
Freshness  = OK
```

Alors :

```text
GLOBAL = WARNING
```

Politique retenue :

```text
WARNING
   ↓
pipeline continue
   ↓
métrique conservée
   ↓
log / notification éventuelle
```

En revanche :

```text
FAILED
   ↓
pipeline bloqué
```

---

# 21. Logging

Les `print()` sont pratiques pendant le développement, mais ils deviennent insuffisants pour l'exploitation d'une plateforme Data.

Le pipeline utilise donc le module Python :

```python
import logging
```

Configuration :

```python
logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s "
        "%(levelname)s "
        "%(name)s - "
        "%(message)s"
    ),
)

logger = logging.getLogger(
    "aviation-pyspark"
)
```

Exemple :

```python
logger.info(
    "RAW loaded raw_count=%s",
    raw_count,
)
```

Résultat :

```text
INFO aviation-pyspark - RAW loaded raw_count=7
```

---

# 22. Logs structurés

Nous privilégions des messages de type :

```text
key=value
```

Exemple :

```python
logger.info(
    "Volume metric expected_count=%s "
    "actual_count=%s "
    "variation_rate=%.2f "
    "status=%s",
    expected_count,
    actual_count,
    volume_variation_rate,
    volume_status,
)
```

Cela produit une information plus facilement exploitable :

```text
expected_count=7
actual_count=7
variation_rate=0.00
status=OK
```

---

# 23. Logs vs métriques

Les logs et les métriques n'ont pas exactement le même rôle.

## Logs

Les logs répondent principalement à :

> Que s'est-il passé ?

Exemples :

```text
Job started
RAW loaded
Airline enrichment completed
Quality Gate FAILED
```

## Métriques

Les métriques répondent principalement à :

> Quel était l'état des données ?

Exemples :

```text
raw_count = 7
rejection_rate = 42.86
duplicate_rate = 25
freshness_minutes = 24.92
```

Les deux sont complémentaires.

---

# 24. Persistance des métriques

Les métriques sont regroupées dans un objet JSON.

Exemple :

```json
{
  "processing_date": "2026-09-03",
  "raw_count": 7,
  "valid_before_dedup": 4,
  "rejected_count": 3,
  "duplicates_removed": 1,
  "valid_after_dedup": 3,
  "rejection_rate": 42.86,
  "duplicate_rate": 25.0,
  "expected_count": 7,
  "volume_variation_rate": 0.0,
  "freshness_minutes": 24.92,
  "rejection_status": "FAILED",
  "duplicate_status": "FAILED",
  "volume_status": "OK",
  "freshness_status": "OK",
  "global_status": "FAILED"
}
```

---

# 25. Emplacement des métriques

Dans la version locale :

```text
data/
└── metrics/
    └── aircraft_positions/
        └── year=2026/
            └── month=09/
                └── day=03/
                    └── quality_metrics.json
```

Exemple :

```text
data/metrics/aircraft_positions/
year=2026/month=09/day=03/
quality_metrics.json
```

---

# 26. Persister avant le Quality Gate

C'est un point architectural essentiel.

La mauvaise séquence serait :

```text
Calcul métriques
      ↓
Quality Gate
      ↓
RuntimeError
      ↓
programme arrêté
      ↓
métriques jamais sauvegardées
```

On perdrait alors le diagnostic au moment où il est le plus utile.

La bonne séquence est :

```text
Calcul métriques
      ↓
Persistance métriques
      ↓
Quality Gate
      ↓
RuntimeError éventuel
```

Ainsi :

```text
job FAILED
mais
diagnostic conservé
```

Principe :

> Les données métier peuvent être bloquées, mais les données d'observabilité doivent survivre à l'échec.

---

# 27. Données Processed et Quality Gate

Le Quality Gate est exécuté avant la publication du lot processed.

Ainsi, si :

```text
GLOBAL STATUS = FAILED
```

le lot n'est pas publié comme un lot sain.

Il faut cependant distinguer :

```text
PROCESSED
```

et :

```text
REJECTED / QUARANTINE
```

En production, conserver les données rejetées même lorsqu'un Quality Gate échoue peut être utile pour analyser l'incident.

---

# 28. Intégration Airflow

Le job PySpark est exécuté depuis Airflow avec un :

```text
BashOperator
```

Architecture :

```text
Airflow
   ↓
BashOperator
   ↓
Python / PySpark
```

Lorsque PySpark exécute :

```python
raise RuntimeError(...)
```

le processus retourne un code non nul.

Le `BashOperator` détecte cet échec.

Résultat :

```text
run_pyspark = FAILED
```

---

# 29. Échec technique vs échec Data Quality

Dans notre test :

```text
run_pyspark = FAILED
```

ne signifie pas nécessairement :

```text
Spark est cassé
```

Le job peut avoir fonctionné exactement comme prévu.

Exemple :

```text
Spark démarre
      ↓
RAW lu correctement
      ↓
métriques calculées
      ↓
qualité insuffisante détectée
      ↓
Quality Gate
      ↓
RuntimeError volontaire
```

Il s'agit donc d'un :

```text
échec Data Quality volontaire
```

et non nécessairement d'une panne technique.

L'observabilité permet précisément de faire cette distinction.

---

# 30. Trigger Rules Airflow

Par défaut, une tâche Airflow en aval attend généralement que ses dépendances aient réussi.

Pour exécuter une tâche de diagnostic lorsqu'une tâche amont échoue, nous utilisons :

```python
trigger_rule="one_failed"
```

Cela signifie :

> Exécuter la tâche lorsqu'au moins une dépendance amont a échoué.

---

# 31. Branche de diagnostic

Notre DAG contient une branche dédiée aux incidents.

Architecture :

```text
start
  ↓
get_processing_date
  ↓
check_raw_file
  ↓
run_pyspark
  |
  +---------------------------+
  |                           |
SUCCESS                     FAILED
  |                           |
  v                           v
check_processed_data    quality_failure_diagnostic
  |
  v
end
```

---

# 32. Diagnostic Airflow

La tâche :

```text
quality_failure_diagnostic
```

lit :

```text
quality_metrics.json
```

et expose les informations importantes.

Exemple réellement observé :

```text
DATA QUALITY FAILURE DIAGNOSTIC

GLOBAL STATUS : FAILED
REJECTION RATE : 42.86%
DUPLICATE RATE : 25.0%
VOLUME VARIATION : 0.0%
RAW COUNT : 7
REJECTED COUNT : 3
```

Lors de notre test Airflow, nous avons obtenu :

```text
run_pyspark                 FAILED
quality_failure_diagnostic  SUCCESS
check_processed_data        UPSTREAM_FAILED
end                         UPSTREAM_FAILED
```

Ce comportement est volontaire.

---

# 33. Pourquoi le diagnostic peut être SUCCESS ?

Cela peut sembler contradictoire :

```text
DAG = FAILED
```

mais :

```text
quality_failure_diagnostic = SUCCESS
```

En réalité, les deux informations concernent deux choses différentes.

Le traitement Data a échoué :

```text
run_pyspark = FAILED
```

mais la tâche chargée d'expliquer l'incident a correctement fonctionné :

```text
quality_failure_diagnostic = SUCCESS
```

C'est donc exactement le comportement attendu.

---

# 34. Stockage local vs stockage partagé

Dans notre environnement de formation, Spark et Airflow utilisent le même environnement WSL.

Ils peuvent donc partager :

```text
/home/matde/projects/.../data/metrics/
```

Mais dans une architecture distribuée :

```text
Machine Spark
Machine Airflow
```

le disque local de Spark n'est pas automatiquement visible par Airflow.

Il faut alors utiliser un stockage partagé et durable.

Exemple :

```text
Spark
  |
  | write
  v
GCS
  ^
  | read
  |
Airflow
```

GCS devient le point de partage.

---

# 35. Architecture production des métriques

Une évolution possible serait :

```text
PySpark
   |
   +--> GCS Processed
   |
   +--> GCS Rejected
   |
   +--> Quality Metrics
             |
             +--> GCS
             |
             +--> BigQuery
                     |
                     v
               Monitoring
```

BigQuery pourrait permettre d'historiser :

```text
processing_date
raw_count
rejection_rate
duplicate_rate
volume_variation
freshness
global_status
```

On pourrait ensuite analyser leur évolution dans le temps.

---

# 36. Monitoring

Le monitoring consiste à suivre l'état du système dans le temps.

Exemples :

```text
rejection_rate
duplicate_rate
freshness
volume
pipeline duration
failed runs
```

Une métrique isolée indique l'état d'un run.

Une série temporelle permet de détecter une tendance.

Exemple :

```text
Jour 1 : rejection_rate = 1 %
Jour 2 : rejection_rate = 2 %
Jour 3 : rejection_rate = 4 %
Jour 4 : rejection_rate = 8 %
Jour 5 : rejection_rate = 15 %
```

Même avant un `FAILED`, cette tendance peut indiquer une dégradation progressive.

---

# 37. Alerting

Le monitoring observe.

L'alerting avertit lorsqu'une condition importante est rencontrée.

Architecture :

```text
Metrics
   ↓
Monitoring
   ↓
Threshold
   ↓
Alert
   ↓
Data Engineer
```

Canaux possibles :

```text
Slack
Teams
e-mail
PagerDuty
```

---

# 38. Politique d'alerting

Politique retenue :

```text
OK
→ pipeline continue
→ aucune alerte

WARNING
→ pipeline continue
→ métriques conservées
→ logs conservés
→ notification non critique éventuelle

FAILED
→ pipeline bloqué
→ diagnostic
→ alerte critique
```

---

# 39. Alert Fatigue

Une alerte ne doit pas être envoyée pour chaque petite anomalie.

Sinon :

```text
trop d'alertes
    ↓
bruit
    ↓
équipe habituée aux alertes
    ↓
alertes ignorées
```

C'est ce qu'on appelle :

```text
Alert Fatigue
```

Les seuils doivent donc correspondre à la criticité réelle.

---

# 40. Exemple d'alerte utile

Une mauvaise alerte serait :

```text
Pipeline failed
```

Elle ne permet pas de diagnostiquer rapidement le problème.

Une meilleure alerte serait :

```text
Toulouse Aviation - Data Quality FAILED

Processing date : 2026-09-03
Rejection rate : 42.86 %
Duplicate rate : 25.00 %
Volume variation : 0.00 %
Freshness : 24.92 min

DAG  : aviation_pipeline
Task : run_pyspark
```

Le Data Engineer possède immédiatement le contexte nécessaire.

---

# 41. Tests automatisés

La logique de Data Quality est couverte avec `pytest`.

Les tests couvrent notamment :

- contrôles de lignes ;
- altitude négative ;
- déduplication ;
- enrichissement ;
- taux de rejet ;
- taux de doublons ;
- variation de volume ;
- freshness ;
- timestamp futur ;
- statuts ;
- statut global ;
- Quality Gate.

---

# 42. Test du Quality Gate

Exemple :

```python
def test_quality_gate_fails_when_global_status_failed():
    with pytest.raises(RuntimeError):
        enforce_quality_gate(
            "FAILED"
        )
```

On vérifie que :

```text
FAILED
```

provoque réellement une exception.

---

# 43. Test d'un WARNING

Exemple :

```python
def test_quality_gate_allows_warning():
    enforce_quality_gate(
        "WARNING"
    )
```

Ce test n'a pas nécessairement besoin d'un `assert`.

Le comportement attendu est simplement :

```text
aucune exception
```

---

# 44. Test des seuils

Exemple rejet :

```python
rate = compute_rejection_rate(
    raw_count=100,
    rejected_count=25,
)

assert rate == 25.0

assert (
    get_rejection_status(rate)
    == "FAILED"
)
```

---

# 45. Test de Freshness

Exemple :

```python
reference_time = datetime.fromisoformat(
    "2026-09-03T13:30:00"
)

latest_ingestion = datetime.fromisoformat(
    "2026-09-03T13:05:00"
)

freshness = compute_freshness_minutes(
    reference_time,
    latest_ingestion,
)

assert freshness == 25.0
assert get_freshness_status(
    freshness
) == "OK"
```

---

# 46. Test du timestamp futur

Le cas découvert pendant notre exécution Airflow est également couvert.

```python
def test_future_timestamp_freshness_failed():
    freshness_minutes = -95.08

    assert (
        get_freshness_status(
            freshness_minutes
        )
        == "FAILED"
    )
```

Cela empêche une régression future du bug.

---

# 47. CI GitHub Actions

Les tests PySpark et Data Quality sont exécutés dans la CI.

La chaîne devient :

```text
Developer
   ↓
git push / Pull Request
   ↓
GitHub Actions
   ↓
pytest
   ↓
Data Quality tests
   ↓
PASS / FAIL
```

Ainsi, une modification cassant la logique du Quality Gate peut être détectée avant intégration.

---

# 48. Quality Gate runtime vs tests CI

Il faut distinguer deux mécanismes.

## Tests CI

La CI exécute les tests unitaires :

```text
pytest
```

Elle vérifie que la logique de Data Quality fonctionne.

## Quality Gate runtime

Lorsqu'Airflow exécute réellement le job :

```text
PySpark
   ↓
données réelles
   ↓
métriques
   ↓
Quality Gate
```

le Quality Gate décide si le lot courant peut continuer.

Les tests CI valident donc le **code du mécanisme**.

Le Quality Gate runtime valide la **qualité du lot traité**.

---

# 49. Architecture complète Data Quality

```text
                       RAW DATA
                          |
                          v
                 +----------------+
                 |     PySpark    |
                 +----------------+
                          |
                          v
                Row Quality Checks
                          |
              +-----------+-----------+
              |                       |
              v                       v
           VALID                   REJECTED
              |
              v
        Deduplication
              |
              v
         Enrichment
              |
              +-------------------------------+
              |                               |
              v                               v
       Quality Metrics                    Logs
              |
              +--> rejection_rate
              |
              +--> duplicate_rate
              |
              +--> volume_variation
              |
              +--> freshness
              |
              v
         Metric Status
              |
              v
         Global Status
              |
              v
       Persist Metrics
              |
              v
         Quality Gate
              |
       +------+------+
       |             |
       v             v
  OK/WARNING       FAILED
       |             |
       v             v
   PROCESSED      RuntimeError
                     |
                     v
                   Airflow
                     |
                     v
          quality_failure_diagnostic
                     |
                     v
                 Monitoring
                     |
                     v
                   Alert
```

---

# 50. Architecture cible GCP

L'évolution vers une architecture plus proche de la production pourrait être :

```text
Producer
   |
   v
Kafka
   |
   v
Consumer
   |
   v
GCS RAW
   |
   v
Spark / Dataproc
   |
   +-------------------+
   |                   |
   v                   v
GCS Processed      GCS Rejected
   |
   v
BigQuery
   |
   v
dbt
   |
   v
Data Marts
```

Observabilité :

```text
Spark
  |
  +--> Logs
  |
  +--> Quality Metrics
           |
           +--> GCS
           |
           +--> BigQuery
                   |
                   v
              Monitoring
                   |
                   v
                Alerts
```

---

# 51. Idempotence et Data Quality

Le traitement utilise une `processing_date`.

Exemple :

```text
2026-09-03
```

Les sorties sont partitionnées :

```text
year=2026/
month=09/
day=03/
```

Le job utilise :

```python
.mode("overwrite")
```

sur la partition précise.

Cela permet de rejouer un traitement sans accumuler automatiquement plusieurs copies du même résultat.

Cette propriété est importante pour :

```text
replay
backfill
late arrival
incident recovery
```

---

# 52. Data Quality et replay

Supposons qu'un lot soit refusé :

```text
2026-09-03
GLOBAL STATUS = FAILED
```

Après correction de la source ou du problème :

```text
nouveau RAW
    ↓
replay 2026-09-03
    ↓
nouveau contrôle qualité
```

Si le nouveau lot est correct :

```text
GLOBAL STATUS = OK
```

la partition peut alors être publiée.

Airflow permet d'orchestrer ce type de replay.

---

# 53. Rejected Data

Les lignes rejetées sont importantes pour l'analyse.

Exemple :

```text
bad001 → ALTITUDE_BELOW_ZERO
bad002 → INVALID_OR_MISSING_ALTITUDE
bad003 → INVALID_OR_MISSING_ALTITUDE
```

Au lieu de simplement supprimer ces lignes, elles peuvent être envoyées vers :

```text
REJECTED
QUARANTINE
DLQ
```

selon le type d'architecture.

Cela facilite :

- diagnostic ;
- correction ;
- audit ;
- replay ;
- analyse de la qualité de la source.

---

# 54. Kafka et Data Quality

Dans l'architecture streaming :

```text
Producer
   ↓
Kafka
   ↓
Consumer
   ↓
GCS RAW
```

le consumer suit une logique :

```text
upload GCS
    ↓
commit Kafka offset
```

Cela permet une sémantique proche de :

```text
at-least-once
```

Un crash peut donc produire des doublons.

La déduplication downstream est donc importante.

La Data Quality ne corrige pas seulement les erreurs métier : elle contribue également à absorber certains effets des mécanismes de livraison distribués.

---

# 55. Metadata de déduplication

Dans notre dataset de formation, nous utilisons :

```text
ingestion_timestamp
```

pour déterminer l'événement à conserver.

Dans une architecture Kafka plus industrielle, on pourrait également conserver :

```text
topic
partition
offset
event_id
ingestion_timestamp
```

Ces métadonnées faciliteraient :

- déduplication ;
- traçabilité ;
- replay ;
- debugging ;
- audit.

---

# 56. Data Observability

La Data Observability va au-delà d'un simple test `NULL`.

Elle cherche à rendre observable la santé du système Data.

Dimensions typiques :

```text
Freshness
Volume
Schema
Distribution
Lineage
Quality
```

Notre projet couvre actuellement principalement :

```text
Completeness
Validity
Uniqueness
Volume
Freshness
```

et pose les bases pour aller plus loin.

---

# 57. Évolutions possibles

Plusieurs améliorations sont possibles.

## Baseline dynamique de volume

Remplacer :

```bash
--expected-count 7
```

par une baseline calculée depuis l'historique.

## Freshness dynamique

En production, utiliser une référence temporelle adaptée au SLA et au mode d'ingestion.

## Métriques dans BigQuery

Créer une table :

```text
data_quality_metrics
```

avec par exemple :

```text
processing_date
dataset
raw_count
rejected_count
rejection_rate
duplicate_rate
volume_variation
freshness_minutes
global_status
generated_at
```

## Dashboard

Créer un dashboard permettant de suivre :

```text
rejection rate
duplicate rate
volume
freshness
failed runs
```

## Alerting

Connecter les incidents critiques à :

```text
Slack
Teams
e-mail
PagerDuty
```

## Data Quality avec dbt

Ajouter des contrôles complémentaires dans les couches :

```text
staging
intermediate
marts
```

---

# 58. Principes retenus

Les principaux principes d'architecture appliqués sont :

```text
Ne pas modifier directement le RAW

Conserver les données rejetées

Calculer des métriques de qualité

Distinguer santé technique et santé Data

Persister les métriques avant le Quality Gate

Faire échouer techniquement le traitement
lorsqu'un seuil critique est dépassé

Conserver le diagnostic après l'échec

Séparer WARNING et FAILED

Éviter l'alert fatigue

Tester automatiquement la logique de qualité

Utiliser un stockage partagé pour les métriques
dans une architecture distribuée
```

---

# 59. Compétences démontrées

Ce module démontre des compétences en :

- Data Quality ;
- Data Observability ;
- PySpark ;
- contrôle de schéma ;
- Completeness ;
- Validity ;
- Uniqueness ;
- Volume monitoring ;
- Freshness monitoring ;
- déduplication ;
- Window functions Spark ;
- Quality Gates ;
- logging Python ;
- métriques structurées ;
- persistance JSON ;
- gestion des rejets ;
- pytest ;
- CI/CD ;
- Airflow ;
- BashOperator ;
- PythonOperator ;
- Trigger Rules ;
- diagnostic d'incident ;
- monitoring ;
- alerting ;
- architecture distribuée ;
- GCS ;
- principes d'idempotence.

---

# 60. Résultat du test réel Airflow

Le pipeline a été testé avec un dataset volontairement dégradé.

Métriques observées :

```text
RAW COUNT          : 7
VALID BEFORE DEDUP : 4
REJECTED           : 3
DUPLICATES REMOVED : 1
VALID AFTER DEDUP  : 3

REJECTION RATE     : 42.86 %
DUPLICATE RATE     : 25.00 %
VOLUME VARIATION   : 0.00 %
```

Le statut global est devenu :

```text
FAILED
```

Le Quality Gate a alors déclenché :

```text
RuntimeError
```

Airflow a obtenu :

```text
run_pyspark                 FAILED
quality_failure_diagnostic  SUCCESS
check_processed_data        UPSTREAM_FAILED
end                         UPSTREAM_FAILED
```

La tâche de diagnostic a correctement récupéré les métriques persistées.

Cette exécution a également permis de détecter un cas limite de freshness négative, conduisant à l'ajout d'une règle spécifique pour les timestamps futurs ou incohérents.

---

# 61. Réponse entretien courte

> Sur mon projet Toulouse Aviation Data Platform, j'ai mis en place une couche de Data Quality dans le pipeline PySpark. Je contrôle notamment la complétude, la validité, les doublons, la volumétrie et la fraîcheur des données. Chaque dimension produit une métrique et un statut OK, WARNING ou FAILED. Les métriques sont persistées avant le Quality Gate afin de conserver le diagnostic même en cas d'échec. Si le statut global est FAILED, le job lève volontairement une exception, ce qui retourne un code non nul à Airflow et bloque la publication du lot. Une branche Airflow utilisant une trigger rule `one_failed` exécute alors une tâche de diagnostic qui récupère les métriques de l'incident. Cette logique est également couverte par des tests pytest exécutés dans la CI.

---

# 62. Réponse entretien détaillée

> J'ai cherché à distinguer la santé technique du pipeline de la santé réelle des données. Un job Spark peut parfaitement s'exécuter alors qu'une proportion importante des données est incorrecte.
>
> J'ai donc implémenté plusieurs contrôles dans PySpark : complétude des champs obligatoires, validité des coordonnées et de l'altitude, détection des doublons, contrôle de volumétrie et freshness.
>
> Les lignes invalides sont séparées des données valides et conservent une `rejection_reason`. Les données valides sont ensuite dédupliquées avec une Window Spark sur `icao24` et `timestamp`, en conservant l'événement ayant le `ingestion_timestamp` le plus récent.
>
> Le pipeline calcule ensuite plusieurs métriques comme le taux de rejet, le taux de doublons, la variation de volume et la freshness. Chaque métrique reçoit un statut OK, WARNING ou FAILED et ces statuts permettent de déterminer un statut global.
>
> Les métriques sont persistées avant l'exécution du Quality Gate. C'est important car, lorsqu'un lot est refusé, je veux conserver les informations qui expliquent pourquoi il a été refusé.
>
> Lorsque le statut global est FAILED, le job lève volontairement une `RuntimeError`. Le processus retourne donc un code non nul et Airflow marque le `BashOperator` comme FAILED.
>
> J'ai ajouté une branche de diagnostic avec `trigger_rule="one_failed"`. Elle s'exécute après l'échec du job, lit les métriques persistées et expose la cause de l'incident.
>
> Lors d'un test réel dans Airflow, cette architecture a également révélé un cas limite : une freshness négative. Le timestamp semblait être situé dans le futur par rapport à l'heure de référence. J'ai donc renforcé la règle et ajouté un test automatisé pour empêcher une régression.
>
> Dans la version locale, les métriques sont stockées dans un fichier JSON. Dans une architecture distribuée, je les stockerais plutôt dans GCS et éventuellement dans BigQuery pour construire un historique, du monitoring et de l'alerting.

---

# 63. Résumé

La chaîne complète mise en œuvre est :

```text
RAW
 ↓
PySpark
 ↓
Quality Checks
 ↓
VALID / REJECTED
 ↓
Deduplication
 ↓
Quality Metrics
 ↓
Rejection Rate
Duplicate Rate
Volume
Freshness
 ↓
Metric Status
 ↓
Global Status
 ↓
Persist Metrics
 ↓
Quality Gate
 ↓
+---------------------------+
|                           |
OK / WARNING              FAILED
|                           |
v                           v
Processed               RuntimeError
                            |
                            v
                         Airflow
                            |
                            v
                quality_failure_diagnostic
                            |
                            v
                      Monitoring
                            |
                            v
                         Alerting
```

L'objectif n'est donc plus seulement :

```text
"Est-ce que mon pipeline tourne ?"
```

mais :

```text
"Est-ce que mon pipeline tourne,
est-ce que les données sont bonnes,
et suis-je capable d'expliquer rapidement
pourquoi elles ne le sont pas ?"
```

C'est le principe central de la **Data Quality & Data Observability**.