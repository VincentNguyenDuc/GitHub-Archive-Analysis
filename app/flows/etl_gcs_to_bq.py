from utils import GCS_FILE_EXTENSION, GCS_BUCKET_NAME, TEMP_PATH
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from datetime import datetime
import pandas as pd
import requests


@task
def extract_from_gcs(dt: datetime, path_to_extract=TEMP_PATH) -> Path:
    """Download data from GCS

    Args:
        dt (datetime): a datetime object with (year, month, day, hour)
        path_to_extract (str): a directory to extract the data. Default to "{CURDIR}/tmp"

    Returns:
        Path: the location of the data
    """
    year, month, day, hour = dt.year, dt.month, dt.day, dt.hour
    gcs_path = f"{year}/{year}-{month:02}-{day:02}-{hour}.{GCS_FILE_EXTENSION}"
    gcs_block = GcsBucket.load(GCS_BUCKET_NAME)

    gcs_block.get_directory(from_path=gcs_path, local_path=path_to_extract)
    return Path(f"{path_to_extract}/{gcs_path}")


@flow()
def etl_gcs_to_bq(dt: datetime) -> None:
    """Load data from Google Cloud Storage to BigQuery

    Args:
        dt (datetime): a datetime object with (year, month, day, hour)
    """
    path = extract_from_gcs(dt)
    print(path)


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
    from etl_web_to_gcs import main_gcs_flow
    main_gcs_flow(2015, 1, [1], [1])
    main_bq_flow(2015, 1, [1], [1])
