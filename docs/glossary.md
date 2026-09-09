# Glossaire --- Module 1

  -----------------------------------------------------------------------
  Terme                   Signification           Rôle dans le projet
  ----------------------- ----------------------- -----------------------
  API                     Application Programming Source permettant de
                          Interface               récupérer les positions
                                                  d'avions

  Producer                Producteur Kafka        Programme qui publie
                                                  des messages dans Kafka

  Python Producer         Producer écrit en       Récupère les positions
                          Python                  via l'API et les publie
                                                  dans Kafka

  Kafka                   Apache Kafka            Plateforme de streaming
                                                  d'événements

  Topic                   Flux logique Kafka      Reçoit les messages
                          nommé                   publiés par les
                                                  producers

  Consumer                Consommateur Kafka      Lit les messages d'un
                                                  topic Kafka

  GCS                     Google Cloud Storage    Stockage objet utilisé
                                                  pour le Data Lake

  Bucket                  Conteneur GCS           Contient les objets
                                                  stockés dans GCS

  RAW                     Données brutes          Données originales
                                                  conservées sans
                                                  transformation

  PySpark                 API Python d'Apache     Nettoyage et
                          Spark                   traitements distribués

  BigQuery                Plateforme analytique   Data Warehouse utilisé
                          Google Cloud            pour l'analyse SQL

  dbt                     data build tool         Modèles SQL,
                                                  dépendances, tests et
                                                  documentation

  Power BI                Outil de Business       Tableaux de bord et
                          Intelligence            visualisation

  Airflow                 Apache Airflow          Orchestration des
                                                  workflows

  Batch                   Traitement par lots     Traitement périodique
                                                  d'un ensemble de
                                                  données

  Streaming               Traitement en flux      Traitement d'événements
                                                  arrivant
                                                  continuellement

  Data Lake               Stockage de données à   Couche de conservation
                          grande échelle          des données, notamment
                                                  RAW

  Data Warehouse          Stockage analytique     Couche destinée aux
                          structuré               requêtes et analyses

  Orchestration           Coordination des        Gestion de l'ordre, des
                          traitements             dépendances et des
                                                  reprises
  -----------------------------------------------------------------------
