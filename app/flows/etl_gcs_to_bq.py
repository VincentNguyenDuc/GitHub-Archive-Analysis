from utils import GCS_FILE_EXTENSION, GCS_BUCKET_NAME
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import pandas as pd
import requests

@task
def extract_from_gcs(year:int, month:int, date:int, hour:int) -> Path:
    """Download data from GCS

    Args:
        year (int): a year
        month (int): a month
        date (int): a date
        hour (int): a hour

    Returns:
        Path: a path
    """
    gcs_path = f"{year}/{year}-{month:02}-{date:02}-{hour}.{GCS_FILE_EXTENSION}"
    gcs_block = GcsBucket.load(GCS_BUCKET_NAME)
    
    local_data_path = "./data"
    gcs_block.get_directory(from_path=gcs_path, local_path=local_data_path)
    return Path(f"{local_data_path}/{gcs_path}")
    
    

@flow()
def etl_gcs_to_bq(year, month, date, hour) -> None:
    """Load data from Google Cloud Storage to BigQuery"""
    path = extract_from_gcs(year, month, date, hour)
    print(path)


@flow(log_prints=True)
def main_bq_flow(
    year: int = 2015,
    month: int = 1,
    dates: list[int] = list(range(1, 32)),
    hours: list[int] = list(range(24))
):
    """Execute multiples ETL flows

    Args:
        year (int, optional): A year. Defaults to 2015.
        month (int, optional): a month. Defaults to 1.
        dates (list[int], optional): dates of a month. Defaults to list(range(1, 32)).
        hours (list[int], optional): hours of a day. Defaults to list(range(24)).
    """
    for d in dates:
        for h in hours:
            etl_gcs_to_bq(year, month, d, h)

if __name__ == "__main__":
    main_bq_flow(2015, 1, [1], [1])