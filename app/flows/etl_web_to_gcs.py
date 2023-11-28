from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from utils import rename_cols, SOURCE_FILE_EXTENSION, GCS_FILE_EXTENSION, GCS_BUCKET_NAME, DATA_SOURCE_URL
from datetime import timedelta, datetime
import requests
import shutil


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(url: str, filename: str) -> pd.DataFrame:
    """
    Read data from https://www.gharchive.org/ into pandas DataFrame

    Args:
        url (str): The URL of the data source
        filename (str): dataset name

    Returns:
        pd.DataFrame: a pandas dataframe
    """
    path = f"./tmp/{filename}.{SOURCE_FILE_EXTENSION}"
    print("\n" + filename + "\n")
    get_response = requests.get(url, stream=True)
    with open(path, 'wb') as f:
        for chunk in get_response.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)

    df = pd.read_json(path, compression='gzip', lines=True)
    return df


@task(retries=3)
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

    clean_data = df.join([actor, repo])
    clean_data.drop(["actor", "repo"], axis=1, inplace=True)

    return clean_data


@task()
def write_local(df: pd.DataFrame, filename: str) -> Path:
    """Write DataFrame out locally as csv gzipped file

    Args:
        df (pd.DataFrame): a dataframe
        filename (str): the dataset file name

    Returns:
        Path: a path object point at the location of the writen file
    """
    path = Path(f"./tmp/{filename}.{GCS_FILE_EXTENSION}")
    df.to_csv(path, compression="gzip")
    return path


@task()
def write_gcs(from_path: Path, to_path: Path) -> None:
    """Upload local file to GCS

    Args:
        path (Path): the path of the file
    """
    gcs_block = GcsBucket.load(GCS_BUCKET_NAME)
    gcs_block.upload_from_path(
        from_path=from_path,
        to_path=to_path
    )
    return None


@task()
def set_up() -> None:
    Path("./tmp").mkdir(parents=True, exist_ok=True)
    return None


@task()
def tear_down() -> None:
    """Tear down temporary folder"""
    shutil.rmtree("./tmp")
    return None


@flow(log_prints=True)
def etl_web_to_gcs(dt: datetime) -> None:
    """The main ETL function"""

    set_up()

    # construct file name and URL
    year, month, day, hour = dt.year, dt.month, dt.day, dt.hour
    filename = f"{year}-{month:02}-{day:02}-{hour}"
    url = f"{DATA_SOURCE_URL}/{filename}.{SOURCE_FILE_EXTENSION}"

    # fetch data
    df = fetch(url, filename)

    # transform
    clean_df = transform_data(df)

    # write to GCS
    from_path = write_local(clean_df, filename)
    to_path = f"{year}/{from_path.name}"
    write_gcs(from_path, to_path)

    tear_down()


@flow(log_prints=True)
def main_gcs_flow(
    year: int = 2015,
    month: int = 1,
    days: list[int] = list(range(1, 32)),
    hours: list[int] = list(range(24))
):
    """Execute multiples ETL flows

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
    main_gcs_flow(2015, 1, [1], [1])
