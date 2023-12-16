"""
Orchestrated the data from source to Google Cloud Storage
"""

from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from constants import GcpConstants, LocalConstants, DataConstants
from utils import tear_down, set_up
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
    with open(path, "wb") as f:
        for chunk in get_response.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)

    df = pd.read_json(
        path,
        compression=DataConstants.COMPRESSION_TYPE,
        lines=True
    )
    return df


@task(name="Transform-Data", log_prints=True)
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Some simple data wrangling

    Args:
        df (pd.DataFrame): a pandas dataframe
    """
    df["actor"] = pd \
        .json_normalize(df["actor"]) \
        .drop(
            labels=["gravatar_id", "display_login", "avatar_url"],
            axis=1
    ).to_dict(orient="records")

    df["org"] = pd \
        .json_normalize(df["org"]) \
        .drop(
            labels=["gravatar_id", "avatar_url"],
            axis=1
    ).to_dict(orient="records")
    return df


@task(name="Write-Local", log_prints=True)
def write_local(df: pd.DataFrame, filename: str) -> [Path]:
    """Write DataFrame out locally as json gzipped file

    Args:
        df (pd.DataFrame): a dataframe
        filename (str): the dataset file name

    Returns:
        [Path]: a list of Path
    """
    paths = []

    for event_name, splitted_df in df.groupby("type"):
        # Construct folders
        directory = f"{LocalConstants.TEMP_PATH}/{event_name}"
        Path(directory).mkdir(parents=True, exist_ok=True)

        path = Path(
            f"{directory}/{filename}.{DataConstants.FILE_EXTENSION}"
        )

        splitted_df.to_json(
            path_or_buf=path,
            orient="records",
            lines=True,
            date_format="iso",
            compression=DataConstants.COMPRESSION_TYPE
        )

        paths.append(path)

    return paths


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
def etl_api_to_gcs(dt: datetime, teardown: bool = True) -> None:
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

    df = transform_data(df)

    # split the data by action types
    from_paths: [Path] = write_local(df, filename)

    # write to gcs
    for path in from_paths:
        path: Path
        event_name = path.parent.name
        file_name = path.name
        write_gcs(
            path,
            Path(f"{event_name}/{file_name}")
        )

    if teardown:
        tear_down()


if __name__ == "__main__":
    for h in range(20, 24):
        dt = datetime(year=2020, month=1, day=1, hour=h)
        etl_api_to_gcs(dt)
