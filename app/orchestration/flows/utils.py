import pandas as pd
from pathlib import Path
from prefect import task
from constants import LocalConstants
import shutil


def rename_cols(df: pd.DataFrame, prefix: str) -> None:
    """Add a prefix to every column of a dataframe

    Args:
        df (pd.DataFrame): a dataframe
        prefix (str): a string
    """
    col_names = df.columns.to_list()
    new_col_names = list(map(lambda name: f"{prefix}_{name}", col_names))
    df.columns = new_col_names


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
