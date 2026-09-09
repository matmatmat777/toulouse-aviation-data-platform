from datetime import datetime, timedelta
from pathlib import Path

import pendulum

from airflow.sdk import Param, dag, get_current_context, task
from airflow.providers.standard.operators.bash import BashOperator


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_DIR = Path(
    "/home/matde/projects/toulouse-aviation-data-platform"
)

PYSPARK_PYTHON = (
    "/home/matde/.venvs/toulouse-aviation/bin/python"
)

SPARK_JOB = (
    f"{PROJECT_DIR}/spark/jobs/process_aircraft_positions.py"
)


# ============================================================
# DAG
# ============================================================

@dag(
    dag_id="aviation_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    tags=["aviation", "training"],
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
        print("Démarrage du pipeline aviation")

    # --------------------------------------------------------
    # TASK 2 - GET PROCESSING DATE
    # --------------------------------------------------------

    @task
    def get_processing_date():

        context = get_current_context()

        processing_date = context["params"]["processing_date"]

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
        retry_delay=timedelta(seconds=10),
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

        return str(raw_file)

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
            "'{{ ti.xcom_pull(task_ids=\"check_raw_file\") }}' "
            "--processing-date "
            "'{{ ti.xcom_pull(task_ids=\"get_processing_date\") }}'"
        ),
        cwd=str(PROJECT_DIR),
        retries=1,
        retry_delay=timedelta(seconds=10),
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
            processed_dir.glob("*.parquet")
        )

        if not parquet_files:
            raise ValueError(
                f"Aucun fichier Parquet trouvé dans : "
                f"{processed_dir}"
            )

        print(
            f"Partition processed : "
            f"{processed_dir}"
        )

        print(
            f"Nombre de fichiers Parquet : "
            f"{len(parquet_files)}"
        )

        return len(parquet_files)

    # --------------------------------------------------------
    # TASK 6 - END
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

    processing_date = get_processing_date()

    raw_file = check_raw_file(
        processing_date
    )

    parquet_count = check_processed_data(
        processing_date
    )

    end_task = end(
        parquet_count,
        processing_date,
    )

    start_task >> processing_date

    raw_file >> run_pyspark >> parquet_count


# ============================================================
# DAG CREATION
# ============================================================

aviation_pipeline()