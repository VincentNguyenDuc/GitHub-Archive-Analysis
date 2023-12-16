"""
Helper Utility Functions
"""

from pathlib import Path
from prefect import task
from constants import LocalConstants
from prefect_gcp.bigquery import SchemaField
import shutil
import json


@task(name="folder-set-up")
def set_up() -> None:
    """Set up temporary folder"""
    Path(LocalConstants.TEMP_PATH).mkdir(parents=True, exist_ok=True)
    return None


@task(name="folder-tear-down")
def tear_down() -> None:
    """Tear down temporary folder"""
    shutil.rmtree(LocalConstants.TEMP_PATH)
    return None


def get_schema(event_name: str) -> list[SchemaField]:
    """Get schema from json file"""
    path = Path(f"./schemas/{event_name}.json")
    with open(path) as json_file:
        event_schema = json.load(json_file)

    return list(map(
        SchemaField.from_api_repr,
        event_schema
    ))


if __name__ == "__main__":
    get_schema("CommitCommentEvent")
