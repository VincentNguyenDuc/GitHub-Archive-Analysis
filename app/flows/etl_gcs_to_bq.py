from utils import tear_down, GcpConstants, LocalConstants, DataConstants
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash
from datetime import datetime, timedelta
import pandas as pd


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch_from_gcs(dt: datetime, path_to_extract=LocalConstants.TEMP_PATH) -> Path:
    """Download data from GCS

    Args:
        dt (datetime): a datetime object with (year, month, day, hour)
        path_to_extract (str): a directory to extract the data. Default to "{CURDIR}/tmp"

    Returns:
        Path: the location of the data
    """
    year, month, day, hour = dt.year, dt.month, dt.day, dt.hour
    gcs_path = f"{year}/{year}-{month:02}-{day:02}-{hour}.{GcpConstants.FILE_EXTENSION}"
    gcs_block = GcsBucket.load(GcpConstants.BUCKET_NAME)

    gcs_block.get_directory(from_path=gcs_path, local_path=path_to_extract)
    return Path(f"{path_to_extract}/{gcs_path}")


@task(retries=3, name="gcs_to_bq")
def transform_data(path: Path) -> pd.DataFrame:
    """Some simple data wrangling

    Args:
        path (Path): location of the data file

    Returns:
        pd.DataFrame: a clean dataframe
    """
    df = pd.read_csv(path, compression=DataConstants.COMPRESSION_TYPE)
    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True)
    return df


@task(retries=3)
def write_bq(df: pd.DataFrame, gbq_table_name="2015") -> None:
    """Write data to BigQuery

    Args:
        df (pd.DataFrame): a dataframe
        gbq_table_name(str): BigQuery table name to write to
    """

    gcp_credentials_block = GcpCredentials.load(GcpConstants.CREDS_NAME)

    df.to_gbq(
        destination_table=f"{GcpConstants.BQ_DATASET}.{gbq_table_name}",
        project_id=GcpConstants.PROJECT_ID,
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )


@flow()
def etl_gcs_to_bq(dt: datetime, teardown: bool = True) -> None:
    """Load data from Google Cloud Storage to BigQuery

    Args:
        dt (datetime): a datetime object with (year, month, day, hour)
        teardown (bool, optional): delete the temporary folder or not. Defaults to True.
    """
    path = fetch_from_gcs(dt)
    df = transform_data(path)
    write_bq(df, dt.year)
    if teardown:
        tear_down()


@flow(log_prints=True)
def main_bq_flow(
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
            etl_gcs_to_bq(dt)


if __name__ == "__main__":
    main_bq_flow(2015, 1, [1], [1])
