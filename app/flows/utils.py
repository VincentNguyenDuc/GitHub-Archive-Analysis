import pandas as pd
from pathlib import Path
from prefect import task
import shutil


class GcpConstants:
    FILE_EXTENSION = "csv.gz"
    BUCKET_NAME = "github-archive-gcs"
    CREDS_NAME = "github-archive-gcp-creds"
    BQ_DATASET = "github_archive_data_all"
    PROJECT_ID = "friendly-basis-406112"


class LocalConstants:
    TEMP_PATH = "./tmp"


class DataConstants:
    FILE_EXTENSION = "json.gz"
    COMPRESSION_TYPE = "gzip"
    SOURCE_URL = "https://data.gharchive.org"


def rename_cols(df: pd.DataFrame, prefix: str) -> None:
    """Add a prefix to every column of a dataframe

    Args:
        df (pd.DataFrame): a dataframe
        prefix (str): a string
    """
    col_names = df.columns.to_list()
    new_col_names = list(map(lambda name: f"{prefix}_{name}", col_names))
    df.columns = new_col_names
    
    
@task()
def set_up() -> None:
    """Set up temporary folder"""
    Path(LocalConstants.TEMP_PATH).mkdir(parents=True, exist_ok=True)
    return None


@task()
def tear_down() -> None:
    """Tear down temporary folder"""
    shutil.rmtree(LocalConstants.TEMP_PATH)
    return None
