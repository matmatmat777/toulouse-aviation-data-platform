import json
from pathlib import Path


DEFAULT_STATE_PATH = Path("data/state/aircraft_positions")


def get_state_path(
    processing_date: str,
    run_id: str,
    base_path: Path = DEFAULT_STATE_PATH,
) -> Path:
    return (
        base_path
        / f"processing_date={processing_date}"
        / f"run_id={run_id}"
        / "state.json"
    )


def save_pipeline_state(
    processing_date: str,
    run_id: str,
    state: dict,
    base_path: Path = DEFAULT_STATE_PATH,
) -> Path:
    state_path = get_state_path(
        processing_date,
        run_id,
        base_path,
    )

    state_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    payload = {
        "processing_date": processing_date,
        "run_id": run_id,
        **state,
    }

    with state_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            payload,
            file,
            indent=2,
            ensure_ascii=False,
        )

    return state_path


def load_pipeline_state(
    processing_date: str,
    run_id: str,
    base_path: Path = DEFAULT_STATE_PATH,
) -> dict:
    state_path = get_state_path(
        processing_date,
        run_id,
        base_path,
    )

    if not state_path.exists():
        return {
            "processing_date": processing_date,
            "run_id": run_id,
            "raw": "NOT_STARTED",
            "pyspark": "NOT_STARTED",
            "bigquery": "NOT_STARTED",
            "dbt": "NOT_STARTED",
        }

    with state_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        return json.load(file)
