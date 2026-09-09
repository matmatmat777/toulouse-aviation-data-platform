# Module 4 — dbt avec BigQuery

## 1. Objectif du module

Ce module a pour objectif de mettre en place une chaîne de transformation analytique avec **dbt Core** et **BigQuery** dans le projet **Toulouse Aviation Data Platform**.

L'architecture obtenue est la suivante :

```text
GCS RAW
   ↓
BigQuery : aviation_raw.aircraft_positions
   ↓ source()
STAGING : stg_aircraft_positions
   [VIEW]
   ↓ ref()
INTERMEDIATE : int_aircraft_positions_deduplicated
   [VIEW]
   ↓ ref()
MART : mart_aircraft_daily_stats
   [TABLE]
   ↓
Power BI
```

dbt ne remplace pas BigQuery :

- **dbt** organise, compile, teste et documente les transformations SQL ;
- **BigQuery** exécute réellement les requêtes SQL.

---

# 2. Pourquoi utiliser dbt ?

Les transformations pourraient être écrites directement dans BigQuery.

Cependant, dbt permet d'industrialiser la couche de transformation SQL grâce à :

- l'organisation des transformations en modèles ;
- la gestion explicite des dépendances ;
- la construction automatique d'un DAG ;
- les tests de qualité ;
- la documentation ;
- le lineage ;
- les matérialisations ;
- Jinja ;
- les macros ;
- l'intégration avec Git et la CI/CD.

dbt est principalement utilisé pour la partie **Transformation** d'une architecture ELT.

---

# 3. Concepts fondamentaux

## 3.1 `source()`

Une `source()` représente une donnée qui existe déjà en dehors des modèles gérés par dbt.

Dans notre projet, la table RAW existe déjà dans BigQuery :

```text
toulouse-aviation-data.aviation_raw.aircraft_positions
```

Elle est déclarée dans :

```text
models/staging/sources.yml
```

avec :

```yaml
version: 2

sources:
  - name: aviation_raw
    database: toulouse-aviation-data
    schema: aviation_raw

    tables:
      - name: aircraft_positions
```

Elle est ensuite utilisée dans un modèle avec :

```sql
from {{ source('aviation_raw', 'aircraft_positions') }}
```

`source()` permet à dbt de connaître la source externe utilisée par le projet et de l'intégrer au lineage.

---

## 3.2 `ref()`

`ref()` sert à référencer un autre modèle dbt.

Exemple :

```sql
from {{ ref('stg_aircraft_positions') }}
```

Cela indique à dbt que le modèle courant dépend de :

```text
stg_aircraft_positions
```

Grâce aux `ref()`, dbt peut construire automatiquement le DAG du projet et déterminer les dépendances entre les modèles.

Il est préférable d'utiliser :

```sql
{{ ref('stg_aircraft_positions') }}
```

plutôt que d'écrire directement :

```sql
`toulouse-aviation-data.aviation_dbt.stg_aircraft_positions`
```

car `ref()` :

- déclare explicitement la dépendance ;
- alimente le DAG ;
- améliore le lineage ;
- évite de coder en dur le nom physique ;
- facilite la gestion des environnements.

---

# 4. DAG dbt

DAG signifie :

**Directed Acyclic Graph — Graphe Orienté Acyclique**

Dans notre projet :

```text
aviation_raw.aircraft_positions
          │
          │ source()
          ▼
stg_aircraft_positions
          │
          │ ref()
          ▼
int_aircraft_positions_deduplicated
          │
          │ ref()
          ▼
mart_aircraft_daily_stats
```

dbt ne se base pas sur les noms :

```text
stg_
int_
mart_
```

pour comprendre l'ordre.

Il analyse les dépendances déclarées principalement avec :

```text
source()
ref()
```

C'est ce qui lui permet de construire le DAG.

---

# 5. Organisation en couches

## SOURCE

Données existantes en entrée de dbt.

Dans notre projet :

```text
aviation_raw.aircraft_positions
```

---

## STAGING

Couche de nettoyage léger et de standardisation.

Objectifs :

- nettoyer les chaînes ;
- normaliser les données ;
- éventuellement renommer les colonnes ;
- harmoniser les types ;
- rester proche de la donnée source.

Dans notre projet :

```text
stg_aircraft_positions
```

---

## INTERMEDIATE

Couche contenant des transformations plus élaborées.

Dans notre projet :

```text
int_aircraft_positions_deduplicated
```

Elle sert notamment à dédupliquer les positions.

---

## MART

Couche finale destinée à la consommation analytique.

Dans notre projet :

```text
mart_aircraft_daily_stats
```

Cette table sera notamment destinée à Power BI.

---

# 6. Installation de dbt

Le projet utilise l'environnement virtuel Python :

```text
.venv
```

Installation :

```powershell
python -m pip install dbt-bigquery
```

Versions utilisées :

```text
dbt Core : 1.12.3
dbt-bigquery : 1.12.0
```

Vérification :

```powershell
dbt --version
```

---

# 7. Initialisation du projet

Depuis le repository :

```powershell
dbt init aviation_dbt
```

Structure obtenue :

```text
aviation_dbt/
├── analyses/
├── macros/
├── models/
├── seeds/
├── snapshots/
├── tests/
├── dbt_project.yml
└── README.md
```

Le projet dbt se trouve dans :

```text
toulouse-aviation-data-platform/
└── aviation_dbt/
```

---

# 8. Sécurité et IAM

Un Service Account dédié aux transformations dbt a été créé :

```text
aviation-dbt-transformer@toulouse-aviation-data.iam.gserviceaccount.com
```

Le principe appliqué est celui du :

**Least Privilege — moindre privilège**

---

## Permissions projet

Rôle :

```text
BigQuery Job User
```

Il permet au Service Account de lancer des jobs BigQuery.

---

## Dataset RAW

Dataset :

```text
aviation_raw
```

Rôle :

```text
BigQuery Data Viewer
```

dbt peut lire les données RAW mais ne doit pas les modifier.

---

## Dataset dbt

Dataset :

```text
aviation_dbt
```

Rôle :

```text
BigQuery Data Editor
```

dbt peut créer et modifier ses vues et tables transformées.

---

# 9. ADC et impersonation

L'authentification utilise :

**Application Default Credentials — ADC**

avec impersonation du Service Account dbt.

Commande :

```powershell
gcloud auth application-default login --impersonate-service-account=aviation-dbt-transformer@toulouse-aviation-data.iam.gserviceaccount.com
```

L'utilisateur doit posséder :

```text
Service Account Token Creator
```

sur ce Service Account.

Vérification :

```powershell
gcloud auth application-default print-access-token
```

Aucune clé JSON de Service Account n'est stockée dans le repository.

À retenir :

```text
ADC
→ permet à l'application d'obtenir une identité

IAM
→ définit ce que cette identité a le droit de faire
```

ADC ne donne pas les permissions.

Les permissions viennent d'IAM.

---

# 10. Configuration `profiles.yml`

Le profil dbt se trouve hors du repository :

```text
C:\Users\matde\.dbt\profiles.yml
```

Configuration utilisée :

```yaml
aviation_dbt:
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: toulouse-aviation-data
      dataset: aviation_dbt
      threads: 4
      timeout_seconds: 300
      location: europe-west9

  target: dev
```

Région :

```text
europe-west9 — Paris
```

Il est important d'utiliser une région cohérente avec les datasets BigQuery.

Validation :

```powershell
dbt debug
```

Résultat :

```text
All checks passed
```

---

# 11. Configuration `dbt_project.yml`

Configuration des matérialisations :

```yaml
name: 'aviation_dbt'
version: '1.0.0'
profile: 'aviation_dbt'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:
  - "target"
  - "dbt_packages"

models:
  aviation_dbt:
    staging:
      +materialized: view

    intermediate:
      +materialized: view

    marts:
      +materialized: table
```

Architecture choisie :

```text
STAGING       → VIEW
INTERMEDIATE  → VIEW
MART          → TABLE
```

---

# 12. Source BigQuery

Fichier :

```text
models/staging/sources.yml
```

Contenu :

```yaml
version: 2

sources:
  - name: aviation_raw
    database: toulouse-aviation-data
    schema: aviation_raw

    tables:
      - name: aircraft_positions
```

La source physique correspond à :

```text
toulouse-aviation-data.aviation_raw.aircraft_positions
```

---

# 13. Modèle staging

Fichier :

```text
models/staging/stg_aircraft_positions.sql
```

Version finale :

```sql
select
    icao24,
    {{ normalize_callsign('callsign') }} as callsign,
    latitude,
    longitude,
    altitude,
    timestamp

from {{ source('aviation_raw', 'aircraft_positions') }}
```

Le staging est matérialisé en :

```text
VIEW
```

Son rôle est de fournir une représentation propre et standardisée des données RAW.

---

# 14. Tests du staging

Fichier :

```text
models/staging/stg_aircraft_positions.yml
```

Contenu :

```yaml
version: 2

models:
  - name: stg_aircraft_positions
    description: "Positions des aéronefs nettoyées à partir des données RAW."

    columns:
      - name: icao24
        description: "Identifiant ICAO24 de l'aéronef."
        data_tests:
          - not_null

      - name: timestamp
        description: "Horodatage de la position de l'aéronef."
        data_tests:
          - not_null
```

Exécution :

```powershell
dbt test --select stg_aircraft_positions
```

Les tests `not_null` ont été validés.

---

# 15. Leçon sur le test `unique`

Un test `unique` a volontairement été essayé sur une colonne qui n'était pas réellement unique.

Le test a échoué.

Cela permet de retenir :

> Un test qui échoue ne signifie pas forcément que la donnée est incorrecte. La règle de qualité peut elle-même être mal définie.

Ni :

```text
icao24
```

ni :

```text
timestamp
```

ne constituent seuls une clé unique suffisante pour une position.

Pour notre jeu de données pédagogique, nous avons retenu comme clé logique :

```text
(icao24, timestamp)
```

Cette clé est adaptée à notre exercice, mais elle n'est pas nécessairement universelle pour tous les flux aéronautiques réels.

---

# 16. Test singulier des doublons

Fichier :

```text
tests/assert_unique_aircraft_position.sql
```

Contenu final :

```sql
select
    icao24,
    timestamp,
    count(*) as number_of_rows

from {{ ref('int_aircraft_positions_deduplicated') }}

group by
    icao24,
    timestamp

having count(*) > 1
```

Principe d'un test singulier dbt :

```text
0 ligne retournée
→ PASS

1 ligne ou plus
→ FAIL
```

Avant déduplication, le test permettait de détecter les doublons.

Après déduplication :

```text
0 doublon
→ PASS
```

---

# 17. Modèle intermediate de déduplication

Fichier :

```text
models/intermediate/int_aircraft_positions_deduplicated.sql
```

Contenu :

```sql
select
    icao24,
    callsign,
    latitude,
    longitude,
    altitude,
    timestamp

from {{ ref('stg_aircraft_positions') }}

qualify row_number() over (
    partition by icao24, timestamp
    order by timestamp
) = 1
```

---

## Fonctionnement de `PARTITION BY`

```sql
partition by icao24, timestamp
```

regroupe les lignes représentant la même position logique.

Exemple :

```text
39abcd + 14:00
    ├── ligne A
    ├── ligne B
    └── ligne C
```

---

## Fonctionnement de `ROW_NUMBER()`

`ROW_NUMBER()` attribue un numéro à chaque ligne du groupe :

```text
ligne A → 1
ligne B → 2
ligne C → 3
```

---

## Fonctionnement de `QUALIFY`

```sql
qualify row_number() ... = 1
```

ne conserve que la première ligne.

Résultat :

```text
39abcd + 14:00
    ├── ligne 1 → conservée
    ├── ligne 2 → supprimée
    └── ligne 3 → supprimée
```

Le test singulier a ensuite validé l'absence de doublons.

---

# 18. Limite de notre déduplication

Dans notre modèle :

```sql
order by timestamp
```

toutes les lignes d'un groupe ont déjà le même timestamp.

L'ordre entre plusieurs doublons n'est donc pas réellement discriminant.

Pour notre exercice, les doublons étant identiques, cela ne pose pas de problème.

Dans un système de production, il serait préférable de disposer d'une information supplémentaire telle que :

```text
ingestion_timestamp
event_id
source_offset
```

Cela permettrait de déterminer précisément quelle ligne conserver.

---

# 19. Mart analytique

Fichier :

```text
models/marts/mart_aircraft_daily_stats.sql
```

Contenu :

```sql
select
    date(timestamp) as flight_date,
    icao24,
    any_value(callsign) as callsign,
    count(*) as position_count,
    min(altitude) as min_altitude,
    max(altitude) as max_altitude,
    avg(altitude) as avg_altitude

from {{ ref('int_aircraft_positions_deduplicated') }}

group by
    flight_date,
    icao24
```

Le mart produit une ligne par :

```text
date + aéronef
```

avec :

- nombre de positions ;
- altitude minimale ;
- altitude maximale ;
- altitude moyenne ;
- callsign représentatif.

---

# 20. `ANY_VALUE`

Nous utilisons :

```sql
any_value(callsign)
```

`ANY_VALUE` permet à BigQuery de prendre une valeur de `callsign` dans le groupe.

Cela évite de devoir ajouter `callsign` au :

```sql
GROUP BY
```

Attention :

> `ANY_VALUE` ne garantit pas quelle valeur sera sélectionnée si plusieurs valeurs différentes existent dans le groupe.

Dans notre exercice, nous supposons que le callsign reste cohérent pour l'aéronef sur la période agrégée.

Si le choix exact de la valeur est important pour le métier, `ANY_VALUE` peut être insuffisant.

---

# 21. Tests du mart

Fichier :

```text
models/marts/mart_aircraft_daily_stats.yml
```

Contenu :

```yaml
version: 2

models:
  - name: mart_aircraft_daily_stats
    description: "Statistiques quotidiennes des positions par aéronef, destinées à la consommation analytique."

    columns:
      - name: flight_date
        description: "Date des positions observées."
        data_tests:
          - not_null

      - name: icao24
        description: "Identifiant ICAO24 de l'aéronef."
        data_tests:
          - not_null

      - name: position_count
        description: "Nombre de positions uniques observées pour l'aéronef."
        data_tests:
          - not_null
```

Nous testons donc également le **produit final destiné à Power BI**.

---

# 22. Matérialisations dbt

## VIEW

Une vue stocke essentiellement la définition de la requête.

Les données sont calculées lors de son interrogation.

Avantages :

- légère ;
- pas de nouvelle copie physique du résultat ;
- adaptée aux transformations intermédiaires simples.

Inconvénient :

- les calculs peuvent être répétés lors des lectures ;
- une transformation complexe peut devenir coûteuse si elle est interrogée souvent.

Dans notre projet :

```text
stg_aircraft_positions
int_aircraft_positions_deduplicated
```

sont des `VIEW`.

---

## TABLE

Une table matérialise physiquement le résultat de la transformation.

Avantages :

- le résultat est stocké ;
- Power BI peut interroger directement le résultat ;
- on évite de recalculer toute la chaîne à chaque interrogation.

Inconvénient :

- la table correspond au dernier build ;
- si de nouvelles données arrivent, il faut reconstruire le modèle pour les intégrer.

Dans notre projet :

```text
mart_aircraft_daily_stats
```

est une `TABLE`.

---

## INCREMENTAL

Une matérialisation `incremental` permet d'éviter de reconstruire tout l'historique à chaque exécution.

Exemple :

```text
Premier chargement :

300 000 000 lignes
        ↓
construction initiale
```

Puis :

```text
Chaque jour :

+ 500 000 lignes
        ↓
traitement des nouvelles données
```

Au lieu de retraiter les 300 millions de lignes quotidiennement.

Avantages :

- moins de données traitées ;
- meilleures performances ;
- réduction potentielle des coûts BigQuery.

---

# 23. `is_incremental()`

Une logique incrémentale peut utiliser :

```sql
{% if is_incremental() %}

...

{% endif %}
```

Exemple pédagogique :

```sql
select *
from {{ ref('int_aircraft_positions_deduplicated') }}

{% if is_incremental() %}

where timestamp > (
    select max(timestamp)
    from {{ this }}
)

{% endif %}
```

`is_incremental()` permet de savoir si le modèle est actuellement exécuté dans le cadre d'un traitement incrémental applicable.

`{{ this }}` représente la relation correspondant au modèle dbt courant.

---

# 24. Risque de `timestamp > max(timestamp)`

Cette stratégie naïve peut perdre des :

**late-arriving events**

Exemple :

```text
MAX(timestamp) déjà présent = 15:00
```

De nouvelles données arrivent :

```text
15:05
15:08
14:55
```

Avec :

```sql
timestamp > max(timestamp)
```

on obtient :

```text
15:05 > 15:00 → prise
15:08 > 15:00 → prise
14:55 > 15:00 → ignorée
```

La position de :

```text
14:55
```

est arrivée en retard.

Le pipeline peut donc afficher :

```text
PASS
```

alors que la donnée est incomplète.

C'est un problème important :

```text
Pipeline techniquement réussi
        ≠
Donnée nécessairement correcte
```

Une stratégie incrémentale robuste doit prendre en compte :

- les late-arriving events ;
- les doublons ;
- les corrections ;
- la rejouabilité ;
- une clé unique fiable ;
- les métadonnées d'ingestion.

Pour cette raison, notre mart reste actuellement en :

```text
TABLE
```

plutôt que de mettre artificiellement en place un incremental mal adapté aux données disponibles.

---

# 25. Jinja dans dbt

Les modèles dbt combinent :

```text
SQL + Jinja
```

Exemple SQL :

```sql
select
    icao24,
    altitude
```

Exemple Jinja :

```jinja
{{ ref('stg_aircraft_positions') }}
```

Processus :

```text
Fichier dbt
SQL + Jinja
     ↓
dbt interprète Jinja
     ↓
dbt produit du SQL
     ↓
BigQuery exécute le SQL
```

BigQuery ne comprend pas directement :

```text
ref()
source()
this
is_incremental()
macros dbt
```

C'est dbt qui interprète et compile ces éléments.

---

# 26. Syntaxes Jinja

## Expression

```jinja
{{ ... }}
```

Exemples :

```jinja
{{ ref('stg_aircraft_positions') }}

{{ source('aviation_raw', 'aircraft_positions') }}

{{ this }}

{{ normalize_callsign('callsign') }}
```

---

## Logique

```jinja
{% ... %}
```

Exemple :

```jinja
{% if is_incremental() %}

...

{% endif %}
```

---

# 27. Macro `normalize_callsign`

Fichier :

```text
macros/normalize_callsign.sql
```

Contenu :

```jinja
{% macro normalize_callsign(column_name) %}

    upper(trim({{ column_name }}))

{% endmacro %}
```

Utilisation :

```sql
{{ normalize_callsign('callsign') }} as callsign
```

La macro permet de centraliser une logique SQL réutilisable.

Sans macro :

```text
15 modèles
→ 15 copies de la même logique
```

Avec macro :

```text
15 modèles
       ↓
normalize_callsign()
       ↓
1 définition
```

Si la règle métier change, on peut modifier la macro à un seul endroit.

Le principe est proche d'une fonction réutilisable en Python.

---

# 28. Compilation dbt

Commande utilisée :

```powershell
dbt compile --select stg_aircraft_positions
```

Nous avions écrit :

```sql
{{ normalize_callsign('callsign') }} as callsign
```

dbt a compilé :

```sql
upper(trim(callsign)) as callsign
```

Nous avions également :

```sql
{{ source('aviation_raw', 'aircraft_positions') }}
```

dbt a compilé :

```sql
`toulouse-aviation-data`.`aviation_raw`.`aircraft_positions`
```

Cela démontre que :

```text
Jinja / macros
      ↓
dbt compile
      ↓
SQL BigQuery
      ↓
BigQuery
```

Après création de notre macro, dbt a également détecté :

```text
561 macros
→
562 macros
```

---

# 29. Commandes dbt utilisées

## Version

```powershell
dbt --version
```

---

## Vérification configuration / connexion

```powershell
dbt debug
```

---

## Parsing

```powershell
dbt parse
```

---

## Construire le staging

```powershell
dbt run --select stg_aircraft_positions
```

---

## Tester le staging

```powershell
dbt test --select stg_aircraft_positions
```

---

## Construire l'intermediate

```powershell
dbt run --select int_aircraft_positions_deduplicated
```

---

## Tester l'intermediate

```powershell
dbt test --select int_aircraft_positions_deduplicated
```

---

## Construire le mart

```powershell
dbt run --select mart_aircraft_daily_stats
```

---

## Construire et tester le projet

```powershell
dbt build
```

---

## Compiler un modèle

```powershell
dbt compile --select stg_aircraft_positions
```

---

## Générer la documentation

```powershell
dbt docs generate
```

---

## Ouvrir la documentation

```powershell
dbt docs serve
```

Arrêt :

```text
Ctrl + C
```

---

# 30. `dbt run` vs `dbt test` vs `dbt build`

## `dbt run`

Construit les modèles dbt.

```text
dbt run
→ modèles
```

---

## `dbt test`

Exécute les tests de qualité.

```text
dbt test
→ tests
```

---

## `dbt build`

Construit les modèles et exécute les tests en respectant le DAG.

```text
dbt build
        ↓
modèles + tests
        ↓
ordre des dépendances
```

Dans notre projet :

```text
staging VIEW
    ↓
tests staging
    ↓
intermediate VIEW
    ↓
test déduplication
    ↓
mart TABLE
    ↓
tests mart
```

---

# 31. Premier `dbt build` complet

Commande :

```powershell
dbt build
```

dbt a détecté :

```text
3 models
3 data tests
1 source
561 macros
```

Ordre observé :

```text
1. stg_aircraft_positions

2. not_null_stg_aircraft_positions_icao24

3. not_null_stg_aircraft_positions_timestamp

4. int_aircraft_positions_deduplicated

5. assert_unique_aircraft_position

6. mart_aircraft_daily_stats
```

Résultat :

```text
PASS=6
WARN=0
ERROR=0
SKIP=0
NO-OP=0
REUSED=0
TOTAL=6
```

Le mart a été matérialisé avec :

```text
CREATE TABLE
3.0 rows
416.0 Bytes processed
```

Les trois lignes correspondent aux trois aéronefs distincts de notre jeu de données après déduplication et agrégation quotidienne.

Des tests supplémentaires ont ensuite été ajoutés au mart et le build est resté valide.

---

# 32. Documentation dbt

Commande :

```powershell
dbt docs generate
```

Résultat :

```text
Building catalog
```

puis :

```text
Catalog written to
aviation_dbt\target\catalog.json
```

Deux avertissements liés à :

```text
table_owner
```

ont été affichés pendant la génération.

Ils n'ont pas empêché la génération de la documentation.

---

# 33. Serveur de documentation

Commande :

```powershell
dbt docs serve
```

La documentation a été ouverte localement dans le navigateur via :

```text
localhost
```

Cette interface permet de consulter :

- les sources ;
- les modèles ;
- les colonnes ;
- les descriptions ;
- les tests ;
- les dépendances ;
- le lineage ;
- le DAG.

---

# 34. Lineage observé

Le lineage permet de visualiser :

```text
aviation_raw.aircraft_positions
          ↓
stg_aircraft_positions
          ↓
int_aircraft_positions_deduplicated
          ↓
mart_aircraft_daily_stats
```

Ce graphe est construit grâce aux :

```text
source()
ref()
```

présents dans les modèles.

---

# 35. Architecture IAM complète à ce stade

Le projet possède plusieurs Service Accounts spécialisés.

## Écriture GCS RAW

```text
aviation-gcs-writer
```

Permission principale :

```text
Storage Object Creator
```

Il peut créer des objets RAW.

Il ne peut pas :

```text
READ
DELETE
REPLACE
```

dans la configuration testée.

---

## Chargement GCS → BigQuery

```text
aviation-bigquery-loader
```

Permissions :

```text
GCS RAW
→ Storage Object Viewer

Projet
→ BigQuery Job User

aviation_raw
→ BigQuery Data Editor
```

---

## Transformations dbt

```text
aviation-dbt-transformer
```

Permissions :

```text
Projet
→ BigQuery Job User

aviation_raw
→ BigQuery Data Viewer

aviation_dbt
→ BigQuery Data Editor
```

Architecture :

```text
aviation-gcs-writer
        ↓
écriture RAW GCS

aviation-bigquery-loader
        ↓
lecture GCS + chargement BigQuery RAW

aviation-dbt-transformer
        ↓
lecture RAW + écriture transformations
```

Cela applique :

- séparation des responsabilités ;
- moindre privilège ;
- identités spécialisées.

---

# 36. Architecture Data obtenue

À la fin du module :

```text
GCS
toulouse-aviation-data-raw
        ↓
BigQuery
aviation_raw.aircraft_positions
        ↓
dbt source()
        ↓
stg_aircraft_positions
VIEW
        ↓
dbt ref()
        ↓
int_aircraft_positions_deduplicated
VIEW
        ↓
dbt ref()
        ↓
mart_aircraft_daily_stats
TABLE
        ↓
future consommation Power BI
```

---

# 37. Bonnes pratiques retenues

- ne jamais modifier directement les données RAW ;
- séparer RAW, transformations et marts ;
- utiliser des Service Accounts spécialisés ;
- appliquer le moindre privilège ;
- éviter les clés JSON statiques lorsque l'impersonation ADC convient ;
- utiliser `source()` pour les données externes à dbt ;
- utiliser `ref()` pour les dépendances entre modèles dbt ;
- ne pas coder inutilement en dur les relations BigQuery ;
- utiliser des tests de qualité ;
- ne pas déclarer une colonne `unique` sans comprendre sa granularité ;
- tester les transformations après correction ;
- dédupliquer avant la consommation analytique ;
- choisir consciemment entre `VIEW`, `TABLE` et `INCREMENTAL` ;
- ne pas utiliser `incremental` simplement parce qu'il semble plus performant ;
- prendre en compte les late-arriving events ;
- centraliser la logique répétitive dans des macros ;
- utiliser `dbt compile` pour comprendre le SQL généré ;
- utiliser le lineage pour comprendre les dépendances ;
- documenter les modèles et colonnes ;
- utiliser `dbt build` pour construire et valider le DAG.

---

# 38. Points d'attention en production

## Déduplication

Notre clé logique pédagogique :

```text
icao24 + timestamp
```

peut être insuffisante dans un système réel.

Une véritable source d'événements devrait idéalement fournir :

```text
event_id
```

ou une autre clé fiable.

---

## Ordre de déduplication

Notre :

```sql
order by timestamp
```

ne permet pas de départager deux événements possédant exactement le même timestamp.

Une donnée comme :

```text
ingestion_timestamp
```

permettrait une sélection déterministe.

---

## Incremental

Un filtre simple :

```sql
timestamp > max(timestamp)
```

peut perdre des événements arrivés en retard.

Il faut concevoir une stratégie capable de gérer :

```text
late arrivals
duplicates
reprocessing
updates
corrections
```

---

## VIEW vs TABLE

Une `VIEW` n'est pas automatiquement meilleure pour un modèle intermédiaire.

Si une transformation devient :

- très coûteuse ;
- très utilisée ;
- complexe ;

une matérialisation physique peut devenir plus intéressante.

Le choix dépend :

```text
volume
coût
fréquence d'utilisation
complexité
fraîcheur attendue
```

---

# 39. Questions d'entretien travaillées

## Quelle différence entre `source()` et `ref()` ?

`source()` référence une donnée qui existe en dehors des modèles dbt.

`ref()` référence un autre modèle géré par dbt.

Les deux participent au lineage.

`ref()` permet notamment à dbt de connaître les dépendances entre ses modèles et de construire le DAG.

---

## Pourquoi utiliser `ref()` plutôt que le nom complet d'une table ?

Parce que `ref()` :

- déclare la dépendance ;
- participe au DAG ;
- permet le lineage ;
- évite de coder en dur la relation ;
- facilite la gestion des environnements.

---

## Pourquoi staging et intermediate en VIEW ?

Ce sont des couches intermédiaires.

Une vue permet d'éviter de matérialiser physiquement chaque transformation lorsque le volume et le coût de calcul restent raisonnables.

---

## Pourquoi le mart en TABLE ?

Le mart est destiné à la consommation analytique.

Power BI peut lire directement un résultat matérialisé sans devoir recalculer toute la chaîne de transformations à chaque requête.

---

## Pourquoi envisager `incremental` ?

Pour éviter de retraiter tout l'historique lorsqu'une faible quantité de nouvelles données arrive.

Exemple :

```text
300 millions de lignes existantes
+
500 000 nouvelles lignes par jour
```

On charge l'historique une première fois puis on traite principalement les nouvelles données.

---

## Quel problème avec `timestamp > max(timestamp)` ?

Une donnée arrivée en retard peut avoir un timestamp inférieur au maximum déjà présent.

Elle sera alors ignorée.

Le job peut :

```text
PASS
```

alors que la donnée est incomplète.

---

## Comment avons-nous détecté les doublons ?

Avec :

```sql
group by
    icao24,
    timestamp

having count(*) > 1
```

dans un test singulier dbt.

---

## Comment avons-nous supprimé les doublons ?

Avec :

```sql
row_number() over (
    partition by icao24, timestamp
    order by timestamp
)
```

puis :

```sql
qualify ... = 1
```

---

## BigQuery exécute-t-il directement Jinja ?

Non.

Le processus est :

```text
SQL + Jinja
     ↓
dbt compile
     ↓
SQL BigQuery
     ↓
BigQuery
```

---

## Différence entre `dbt run`, `dbt test` et `dbt build`

```text
dbt run
→ construit les modèles

dbt test
→ exécute les tests

dbt build
→ construit et teste en respectant le DAG
```

---

# 40. Compétences acquises

À l'issue de ce module, les compétences suivantes ont été pratiquées :

- installation de dbt Core ;
- utilisation de `dbt-bigquery` ;
- initialisation d'un projet dbt ;
- configuration de `profiles.yml` ;
- connexion à BigQuery ;
- authentification avec ADC ;
- impersonation d'un Service Account ;
- IAM et moindre privilège ;
- création d'un dataset de transformation ;
- déclaration d'une source ;
- utilisation de `source()` ;
- utilisation de `ref()` ;
- compréhension du DAG ;
- architecture Source / Staging / Intermediate / Mart ;
- matérialisation `VIEW` ;
- matérialisation `TABLE` ;
- compréhension de `INCREMENTAL` ;
- compréhension de `is_incremental()` ;
- compréhension de `{{ this }}` ;
- compréhension des late-arriving events ;
- tests génériques `not_null` ;
- tests d'unicité ;
- tests singuliers ;
- détection des doublons ;
- déduplication avec `ROW_NUMBER()` ;
- utilisation de `PARTITION BY` ;
- utilisation de `QUALIFY` ;
- agrégations BigQuery ;
- utilisation de `ANY_VALUE` ;
- Jinja ;
- macros ;
- compilation SQL ;
- `dbt run` ;
- `dbt test` ;
- `dbt build` ;
- `dbt compile` ;
- `dbt docs generate` ;
- `dbt docs serve` ;
- documentation des modèles ;
- visualisation du lineage ;
- explication des choix techniques en entretien.

---

# 41. Commandes récapitulatives

```powershell
dbt --version
```

```powershell
dbt debug
```

```powershell
dbt parse
```

```powershell
dbt run --select stg_aircraft_positions
```

```powershell
dbt test --select stg_aircraft_positions
```

```powershell
dbt run --select int_aircraft_positions_deduplicated
```

```powershell
dbt test --select int_aircraft_positions_deduplicated
```

```powershell
dbt run --select mart_aircraft_daily_stats
```

```powershell
dbt build
```

```powershell
dbt compile --select stg_aircraft_positions
```

```powershell
dbt docs generate
```

```powershell
dbt docs serve
```

---

# 42. Résultat final du Module 4

Le pipeline dbt est opérationnel :

```text
BigQuery RAW
aviation_raw.aircraft_positions
        ↓
source()
        ↓
stg_aircraft_positions
VIEW
        ↓
ref()
        ↓
int_aircraft_positions_deduplicated
VIEW
        ↓
ref()
        ↓
mart_aircraft_daily_stats
TABLE
        ↓
Power BI
```

Les transformations sont :

```text
organisées
+
testées
+
documentées
+
liées par un DAG
+
compilées en SQL BigQuery
+
exécutables avec dbt build
```

Le projet dispose désormais d'une véritable couche de transformation analytique avec :

**BigQuery + dbt Core + Data Quality + Lineage + IAM + Jinja/macros.**

---

# 43. Validation du module

Module 4 — **dbt : VALIDÉ**

Les principaux concepts sont compris et ont été appliqués dans un projet réel :

```text
source()
ref()
DAG
staging
intermediate
mart
VIEW
TABLE
INCREMENTAL
tests
ROW_NUMBER
PARTITION BY
QUALIFY
Jinja
macros
compile
build
docs
lineage
IAM
ADC
BigQuery
```

Prochaine étape du parcours :

```text
Module 5 — Kafka
```