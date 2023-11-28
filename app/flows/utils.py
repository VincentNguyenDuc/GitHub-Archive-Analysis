import pandas as pd

SOURCE_FILE_EXTENSION = "json.gz"
GCS_FILE_EXTENSION = "csv.gz"
GCS_BUCKET_NAME = "github-archive-gcs"
DATA_SOURCE_URL = "https://data.gharchive.org"
TEMP_PATH = "./tmp"


def rename_cols(df: pd.DataFrame, prefix: str) -> None:
    """Add a prefix to every column of a dataframe

    Args:
        df (pd.DataFrame): a dataframe
        prefix (str): a string
    """
    col_names = df.columns.to_list()
    new_col_names = list(map(lambda name: f"{prefix}.{name}", col_names))
    df.columns = new_col_names
