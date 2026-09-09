# Architecture --- Module 1

## Besoin

La plateforme doit récupérer des positions d'avions en continu et des
référentiels périodiques, conserver les données originales, les
nettoyer, les rendre interrogeables et produire des modèles analytiques
pour Power BI.

## Architecture V1

``` text
                     SOURCES
              ┌─────────┴─────────┐
              │                   │
          STREAMING             BATCH
              │                   │
      Positions avions       Référentiels
              │             aéroports, etc.
              ▼                   ▼
       Python Producer          Python
              │                   │
              ▼                   │
         Kafka Topic              │
              │                   │
              ▼                   │
       Python Consumer            │
              └─────────┬─────────┘
                        ▼
                     GCS RAW
                        │
                        ▼
                     PySpark
        nettoyage / standardisation /
        dédoublonnage / enrichissement
                        │
                        ▼
                    BigQuery
                        │
                        ▼
                       dbt
          modèles analytiques / tests
                        │
                        ▼
                    Power BI

Airflow : orchestration des workflows et dépendances.
```

## Décisions d'architecture

### Pourquoi Kafka ?

Les positions d'avions constituent un flux continu d'événements. Kafka
permet de publier ces événements dans un flux logique et de découpler le
producteur des consommateurs.

### Pourquoi un Python Producer ?

Le programme Python récupère les positions depuis l'API aéronautique et
publie chaque événement dans Kafka.

### Pourquoi un Consumer ?

Le consumer lit les événements publiés dans Kafka et permet notamment
leur persistance vers la couche de stockage.

### Pourquoi GCS ?

Google Cloud Storage constitue la couche de stockage objet du Data Lake.
Les données RAW y sont conservées sans transformation afin de pouvoir
rejouer les traitements si nécessaire.

### Pourquoi conserver le RAW ?

Une erreur dans un traitement ne doit pas détruire la donnée source. Le
RAW permet de corriger le traitement puis de reconstruire les données
dérivées.

### Pourquoi PySpark ?

PySpark assure les traitements de données à grande échelle :
dédoublonnage, gestion des valeurs invalides ou nulles, standardisation,
conversion des timestamps, enrichissement et calculs.

### Pourquoi BigQuery ?

BigQuery constitue la couche Data Warehouse / analytique. Les données
structurées peuvent y être interrogées efficacement en SQL.

### Pourquoi dbt ?

dbt organise les transformations SQL dans BigQuery sous forme de modèles
maintenables. Il servira également aux tests et à la documentation des
modèles.

### Pourquoi Power BI ?

Power BI exploite les données finales préparées pour produire les
tableaux de bord et analyses.

### Pourquoi Airflow ?

Airflow orchestre les traitements, leurs dépendances, leurs exécutions
et les reprises en cas d'échec. Il n'effectue pas lui-même les calculs
PySpark.

## Batch et streaming

Le projet utilise les deux approches :

-   **Streaming** pour les positions d'avions reçues continuellement.
-   **Batch** pour les référentiels récupérés périodiquement, par
    exemple la liste des aéroports.

Les données reçues en streaming peuvent également être persistées puis
retraitées ultérieurement par lots.
