"""
Orchestrated the data from Google Cloud Storage to Google BigQuery
"""

from constants import GcpConstants, LocalConstants, DataConstants
from utils import tear_down, set_up
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import bigquery_create_table, TimePartitioning, SchemaField, BigQueryWarehouse
from prefect_gcp import GcpCredentials
from datetime import datetime
import pandas as pd


@task(
    name="Fetch-From-GCS",
    retries=3
)
def fetch_from_gcs(dt: datetime, path_to_extract=LocalConstants.TEMP_PATH) -> Path:
    """Download data from GCS

    Args:
        dt (datetime): a datetime object with (year, month, day, hour)
        path_to_extract (str): a directory to extract the data. Default to "{CURDIR}/tmp"

    Returns:
        Path: the location of the data
    """
    year, month, day, hour = dt.year, dt.month, dt.day, dt.hour
    gcs_path = f"{year}/{month}/{day}/{year}-{month:02}-{day:02}-{hour}.{GcpConstants.FILE_EXTENSION}"
    gcs_block = GcsBucket.load(GcpConstants.BUCKET_NAME)

    gcs_block.get_directory(from_path=gcs_path, local_path=path_to_extract)
    return Path(f"{path_to_extract}/{gcs_path}")


@task(name="Transform-Data-From-GCS")
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

    try:
        df.drop("actor_display_login", axis=1, inplace=True)
    except KeyError:
        pass

    df["created_at"] = pd \
        .to_datetime(df["created_at"]) \
        .dt \
        .tz_localize(None)

    return df


@flow(name="GCS-To-BQ")
def etl_gcs_to_bq(dt: datetime, teardown: bool = True) -> None:
    """Load data from Google Cloud Storage to BigQuery

    Args:
        dt (datetime): a datetime object with (year, month, day, hour)
        teardown (bool, optional): delete the temporary folder or not. Defaults to True.
    """

    # set up the folder
    set_up()

    # fetch data from gcs
    path = fetch_from_gcs(dt)

    # transform the data
    df = transform_data(path)

    # Table Information and Schema
    table_name = str(dt.year)
    schema = [
        {"name": "id", "type": "INTEGER"},
        {"name": "type", "type": "STRING"},
        {"name": "public", "type": "BOOLEAN"},
        {"name": "created_at", "type": "TIMESTAMP"},
        {"name": "actor_id", "type": "INTEGER"},
        {"name": "actor_login", "type": "STRING"},
        {"name": "actor_url", "type": "STRING"},
        {"name": "actor_avatar_url", "type": "STRING"},
        {"name": "repo_id", "type": "INTEGER"},
        {"name": "repo_name", "type": "STRING"},
        {"name": "repo_url", "type": "STRING"}
    ]
    clustering_fields = ["repo_name", "type", "id"]
    time_partitoning = TimePartitioning(field="created_at")  # default by DAY
    gcp_creds:GcpCredentials = GcpCredentials.load(GcpConstants.CREDS_NAME)

    # Create table if not exists
    bigquery_create_table(
        dataset=GcpConstants.BQ_DATASET,
        table=table_name,
        gcp_credentials=gcp_creds,
        schema=list(map(SchemaField.from_api_repr, schema)),
        time_partitioning=time_partitoning,
        clustering_fields=clustering_fields,
        location=GcpConstants.LOCATION
    )

    # write data to table, wrap it around a task decorator
    @task(name="Upload-Data", retries=3)
    def to_gbq_wrapper():
        df.to_gbq(
            destination_table=f"{GcpConstants.BQ_DATASET}.{table_name}",
            project_id=GcpConstants.PROJECT_ID,
            credentials=gcp_creds.get_credentials_from_service_account(),
            chunksize=500_000,
            if_exists="append",
            table_schema=schema,
            location=GcpConstants.LOCATION
        )
    
    to_gbq_wrapper()

    if teardown:
        tear_down()


@flow(name="Main-BQ-Flow")
def main_bq_flow(
    year: int = 2015,
    month: int = 1,
    days: list[int] = list(range(1, 32)),
    hours: list[int] = list(range(24))
):
    """Execute multiples ETL flows from Cloud Storage to BigQuery

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
    main_bq_flow(year=2015, month=1, days=[1, 2, 3], hours=[1])
