import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import pendulum

from airflow.sdk import Param, dag, get_current_context, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


# ============================================================
# CONFIGURATION
# ============================================================

# Racine du projet détectée automatiquement à partir de ce fichier :
#
# airflow/dags/aviation_pipeline.py
#        ↑
# airflow/dags
# airflow
# projet
#
# Une variable d'environnement permet néanmoins de la surcharger
# en DEV, PROD, Docker, VM, etc.

DEFAULT_PROJECT_DIR = (
    Path(__file__)
    .resolve()
    .parents[2]
)

PROJECT_DIR = Path(
    os.getenv(
        "AVIATION_PROJECT_DIR",
        str(DEFAULT_PROJECT_DIR),
    )
).resolve()


# Le DAG Airflow et PySpark utilisent actuellement deux
# environnements Python différents.
#
# Cette valeur peut être remplacée par une variable
# d'environnement selon l'environnement d'exécution.

DEFAULT_PYSPARK_PYTHON = (
    Path.home()
    / ".venvs"
    / "toulouse-aviation"
    / "bin"
    / "python"
)

PYSPARK_PYTHON = os.getenv(
    "AVIATION_PYSPARK_PYTHON",
    str(DEFAULT_PYSPARK_PYTHON),
)


# Chemin du job Spark construit à partir de la racine du projet.

SPARK_JOB = (
    PROJECT_DIR
    / "spark"
    / "jobs"
    / "process_aircraft_positions.py"
)


# ============================================================
# DATA QUALITY FAILURE DIAGNOSTIC
# ============================================================

def read_quality_failure_metrics(
    processing_date: str,
):
    year, month, day = processing_date.split("-")

    metrics_path = (
        PROJECT_DIR
        / "data"
        / "metrics"
        / "aircraft_positions"
        / f"year={year}"
        / f"month={month}"
        / f"day={day}"
        / "quality_metrics.json"
    )

    if not metrics_path.exists():
        raise FileNotFoundError(
            f"Quality metrics not found: {metrics_path}"
        )

    with metrics_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        metrics = json.load(file)

    print("=" * 60)
    print("DATA QUALITY FAILURE DIAGNOSTIC")
    print("=" * 60)

    print(
        f"GLOBAL STATUS : "
        f"{metrics.get('global_status')}"
    )

    print(
        f"REJECTION RATE : "
        f"{metrics.get('rejection_rate')}%"
    )

    print(
        f"DUPLICATE RATE : "
        f"{metrics.get('duplicate_rate')}%"
    )

    print(
        f"VOLUME VARIATION : "
        f"{metrics.get('volume_variation_rate')}%"
    )

    print(
        f"FRESHNESS : "
        f"{metrics.get('freshness_minutes')} min"
    )

    print(
        f"RAW COUNT : "
        f"{metrics.get('raw_count')}"
    )

    print(
        f"REJECTED COUNT : "
        f"{metrics.get('rejected_count')}"
    )

    print("=" * 60)


# ============================================================
# DAG
# ============================================================

@dag(
    dag_id="aviation_pipeline",
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        9,
        1,
        tz="UTC",
    ),
    catchup=False,
    tags=[
        "aviation",
        "training",
    ],
    params={
        "processing_date": Param(
            "2026-09-03",
            type="string",
            format="date",
            description="Date des données à traiter",
        )
    },
)
def aviation_pipeline():

    # --------------------------------------------------------
    # TASK 1 - START
    # --------------------------------------------------------

    @task
    def start():
        print(
            "Démarrage du pipeline aviation"
        )

    # --------------------------------------------------------
    # TASK 2 - GET PROCESSING DATE
    # --------------------------------------------------------

    @task
    def get_processing_date():

        context = get_current_context()

        processing_date = (
            context["params"]["processing_date"]
        )

        print(
            f"Date de traitement : "
            f"{processing_date}"
        )

        return processing_date

    # --------------------------------------------------------
    # TASK 3 - CHECK RAW FILE
    # --------------------------------------------------------

    @task(
        retries=2,
        retry_delay=timedelta(
            seconds=10
        ),
    )
    def check_raw_file(
        processing_date: str,
    ):

        date = datetime.strptime(
            processing_date,
            "%Y-%m-%d",
        )

        raw_file = (
            PROJECT_DIR
            / "data/raw"
            / f"year={date.year}"
            / f"month={date.month:02d}"
            / f"day={date.day:02d}"
            / "aircraft_positions.jsonl"
        )

        print(
            f"Date demandée : "
            f"{processing_date}"
        )

        print(
            f"Partition RAW : "
            f"{raw_file}"
        )

        if not raw_file.exists():
            raise FileNotFoundError(
                f"Fichier RAW introuvable : "
                f"{raw_file}"
            )

        size = raw_file.stat().st_size

        print(
            f"Fichier RAW trouvé : "
            f"{raw_file}"
        )

        print(
            f"Taille : "
            f"{size} octets"
        )

        return str(
            raw_file
        )

    # --------------------------------------------------------
    # TASK 4 - RUN PYSPARK
    # --------------------------------------------------------

    run_pyspark = BashOperator(
        task_id="run_pyspark",
        bash_command=(
            "set -e; "
            f"{PYSPARK_PYTHON} "
            f"{SPARK_JOB} "
            "--input "
            "'{{ ti.xcom_pull("
            "task_ids=\"check_raw_file\") }}' "
            "--processing-date "
            "'{{ ti.xcom_pull("
            "task_ids=\"get_processing_date\") }}' "
            "--expected-count 7 "
            "--reference-time "
            "'{{ ti.xcom_pull("
            "task_ids=\"get_processing_date\") }}T13:30:00' "
            "--run-id "
            "'{{ run_id }}'"
        ),
        cwd=str(
            PROJECT_DIR
        ),
        retries=1,
        retry_delay=timedelta(
            seconds=10
        ),
    )

    # --------------------------------------------------------
    # TASK 5 - CHECK PROCESSED DATA
    # --------------------------------------------------------

    @task
    def check_processed_data(
        processing_date: str,
    ):

        date = datetime.strptime(
            processing_date,
            "%Y-%m-%d",
        )

        processed_dir = (
            PROJECT_DIR
            / "data/processed/aircraft_positions"
            / f"year={date.year}"
            / f"month={date.month:02d}"
            / f"day={date.day:02d}"
        )

        if not processed_dir.exists():
            raise FileNotFoundError(
                f"Données processed introuvables : "
                f"{processed_dir}"
            )

        parquet_files = list(
            processed_dir.glob(
                "*.parquet"
            )
        )

        if not parquet_files:
            raise ValueError(
                "Aucun fichier Parquet trouvé dans : "
                f"{processed_dir}"
            )

        print(
            f"Partition processed : "
            f"{processed_dir}"
        )

        print(
            "Nombre de fichiers Parquet : "
            f"{len(parquet_files)}"
        )

        return len(
            parquet_files
        )

    # --------------------------------------------------------
    # TASK 6 - QUALITY FAILURE DIAGNOSTIC
    # --------------------------------------------------------

    quality_failure_diagnostic = PythonOperator(
        task_id="quality_failure_diagnostic",
        python_callable=read_quality_failure_metrics,
        op_kwargs={
            "processing_date": (
                "{{ params.processing_date }}"
            ),
        },
        trigger_rule="one_failed",
    )

    # --------------------------------------------------------
    # TASK 7 - END
    # --------------------------------------------------------

    @task
    def end(
        parquet_count: int,
        processing_date: str,
    ):

        print(
            "Pipeline aviation terminé "
            "avec succès."
        )

        print(
            f"Date traitée : "
            f"{processing_date}"
        )

        print(
            f"{parquet_count} fichier(s) "
            "Parquet validé(s)."
        )

    # --------------------------------------------------------
    # DEPENDENCIES
    # --------------------------------------------------------

    start_task = start()

    processing_date = (
        get_processing_date()
    )

    raw_file = check_raw_file(
        processing_date
    )

    parquet_count = (
        check_processed_data(
            processing_date
        )
    )

    end_task = end(
        parquet_count,
        processing_date,
    )

    start_task >> processing_date

    raw_file >> run_pyspark

    run_pyspark >> parquet_count

    run_pyspark >> quality_failure_diagnostic


# ============================================================
# DAG CREATION
# ============================================================

aviation_pipeline()