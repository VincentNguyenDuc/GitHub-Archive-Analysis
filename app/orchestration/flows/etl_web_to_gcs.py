"""
Orchestrated the data from source to Google Cloud Storage
"""

from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from constants import GcpConstants, LocalConstants, DataConstants
from utils import tear_down, set_up, rename_cols
from datetime import timedelta, datetime
import requests


@task(
    name="Fetch-Data-Source",
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
    log_prints=True
)
def fetch_from_source(url: str, filename: str) -> pd.DataFrame:
    """
    Read data from https://www.gharchive.org/ into pandas DataFrame

    Args:
        url (str): The URL of the data source
        filename (str): dataset name

    Returns:
        pd.DataFrame: a pandas dataframe
    """
    path = f"{LocalConstants.TEMP_PATH}/{filename}.{DataConstants.FILE_EXTENSION}"
    print("\n" + filename + "\n")
    get_response = requests.get(url, stream=True)
    with open(path, 'wb') as f:
        for chunk in get_response.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)

    df = pd.read_json(
        path, compression=DataConstants.COMPRESSION_TYPE, lines=True)
    return df


@task(
    name="Transform-Data-From-Source",
    retries=3,
    log_prints=True
)
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform a simple data wrangling

    Args:
        df (pd.DataFrame): a dataframe

    Returns:
        pd.DataFrame: clean dataframe
    """

    df.drop(["payload", "org"], axis=1, inplace=True)

    actor = pd.json_normalize(df["actor"])
    actor = actor.drop("gravatar_id", axis=1)
    rename_cols(actor, "actor")

    repo = pd.json_normalize(df["repo"])
    rename_cols(repo, "repo")

    clean_df = df.join([actor, repo])
    clean_df.drop(["actor", "repo"], axis=1, inplace=True)

    return clean_df


@task(name="Write-Local", log_prints=True)
def write_local(df: pd.DataFrame, filename: str) -> Path:
    """Write DataFrame out locally as csv gzipped file

    Args:
        df (pd.DataFrame): a dataframe
        filename (str): the dataset file name

    Returns:
        Path: a path object point at the location of the writen file
    """
    path = Path(
        f"{LocalConstants.TEMP_PATH}/{filename}.{GcpConstants.FILE_EXTENSION}")
    df.to_csv(path, compression=DataConstants.COMPRESSION_TYPE, index=False)
    return path


@task(name="Write-GCS", log_prints=True)
def write_gcs(from_path: Path, to_path: Path) -> None:
    """Upload local file to GCS

    Args:
        path (Path): the path of the file
    """
    gcs_block = GcsBucket.load(GcpConstants.BUCKET_NAME)
    gcs_block.upload_from_path(
        from_path=from_path,
        to_path=to_path
    )
    return None


@flow(name="Data-Source-To-GCS", retries=3, log_prints=True)
def etl_web_to_gcs(dt: datetime, teardown: bool = True) -> None:
    """The main ETL function

    Args:
        dt (datetime): a datetime object with year, month, day, hour
        teardown (bool, optional): delete the temporary folder or not. Defaults to True.
    """

    set_up()

    # construct file name and URL
    year, month, day, hour = dt.year, dt.month, dt.day, dt.hour
    filename = f"{year}-{month:02}-{day:02}-{hour}"
    url = f"{DataConstants.SOURCE_URL}/{filename}.{DataConstants.FILE_EXTENSION}"

    # fetch data
    df = fetch_from_source(url, filename)

    # transform
    clean_df = transform_data(df)

    # write to GCS
    from_path = write_local(clean_df, filename)
    to_path = f"{year}/{month}/{day}/{from_path.name}"
    write_gcs(from_path, to_path)

    if teardown:
        tear_down()


@flow(name="Main-GCS-Flow", log_prints=True)
def main_gcs_flow(
    year: int = 2015,
    month: int = 1,
    days: list[int] = list(range(1, 32)),
    hours: list[int] = list(range(24))
):
    """Execute multiples ETL flows from Data Source to Cloud Storage

    Args:
        year (int, optional): A year. Defaults to 2015.
        month (int, optional): a month. Defaults to 1.
        days (list[int], optional): days of a month. Defaults to list(range(1, 32)).
        hours (list[int], optional): hours of a day. Defaults to list(range(24)).
    """
    for d in days:
        for h in hours:
            dt = datetime(year=year, month=month, day=d, hour=h)
            etl_web_to_gcs(dt)


if __name__ == "__main__":
    main_gcs_flow(year=2015, month=1, days=[1, 2, 3], hours=[1])
