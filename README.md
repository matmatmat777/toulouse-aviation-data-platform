# Toulouse Aviation Data Platform

Projet fil rouge de Data Engineering orienté aéronautique.

## Objectif

Construire une plateforme Data capable d'ingérer des positions d'avions
en streaming, de conserver les données brutes, de les nettoyer et de les
transformer afin de produire des données analytiques exploitables dans
Power BI.

## Architecture V1

API Aviation → Python Producer → Kafka → Python Consumer → GCS RAW →
PySpark → BigQuery → dbt → Power BI

Airflow orchestre les traitements et leurs dépendances.

## Sources de données

-   **Streaming** : positions d'avions récupérées en continu.
-   **Batch** : référentiels tels que les aéroports, compagnies et
    avions, récupérés périodiquement.

## Documentation

-   `docs/architecture/architecture.md` : architecture et décisions
    techniques.
-   `docs/glossary.md` : vocabulaire du projet.

## État du projet

-   [x] Module 1 --- Architecture Data moderne
-   [ ] Module 2 --- Fondamentaux GCP
-   [ ] Module 3 --- BigQuery
-   [ ] Module 4 --- dbt
-   [ ] Module 5 --- Kafka
-   [ ] Module 6 --- PySpark
-   [ ] Module 7 --- Airflow
-   [ ] Module 8 --- Terraform
-   [ ] Module 9 --- Docker
-   [ ] Module 10 --- CI/CD
-   [ ] Module 11 --- Data Quality
-   [ ] Module 12 --- Databricks
-   [ ] Module 13 --- Snowflake
-   [ ] Module 14 --- Industrialisation
-   [ ] Module 15 --- Portfolio et entretien
