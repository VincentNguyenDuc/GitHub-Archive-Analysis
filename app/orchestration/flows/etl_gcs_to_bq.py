"""
Orchestrated the data from Google Cloud Storage to Google BigQuery
"""

from constants import GcpConstants, LocalConstants, DataConstants
from utils import tear_down, set_up
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import datetime, timedelta
import pandas as pd


@task(
    name="Fetch-From-GCS",
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1)
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


@task(
    name="Transform-Data-From-GCS",
    retries=3
)
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


@task(name="Create-BQ-Table")
def create_table(
    gbq_table_name: str = "2015",
    schema: list[bigquery.SchemaField] = None,
    clustering_fields: list[str] = None,
    time_partitioning: bigquery.TimePartitioning = None
):
    """Create a table in Google Big Query Dataset.
    If the table is already exist, then do nothing

    Args:
        gbq_table_name (str, optional): table name. Defaults to "2015".
        schema (list[bigquery.SchemaField], optional): schema of the table. Defaults to None.
        clustering_fields (list[str], optional): clustering. Defaults to None.
        time_partitioning (bigquery.TimePartitioning, optional): partition. Defaults to None.
    """
    client = bigquery.Client(
        project=GcpConstants.PROJECT_ID,
        credentials=GcpCredentials.load(GcpConstants.CREDS_NAME)
    )
    table_id = f"{GcpConstants.BQ_DATASET}.{gbq_table_name}"

    try:
        client.get_table(table_id)
        print("Table {} already exists. Do nothing".format(table_id))
    except NotFound:
        table = bigquery.Table(table_id, schema)
        table.clustering_fields = clustering_fields
        table.time_partitioning = time_partitioning
        created_table = client.create_table(table)
        print(
            "Created table {}.{}.{}".format(
                created_table.project, created_table.dataset_id, created_table.table_id
            )
        )


@task(name="Write-BQ", retries=3)
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


@flow(
    name="GCS-To-BQ",
    retries=3
)
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

    # create table and specify partition and cluster
    table_name = str(dt.year)
    schema = [
        bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("public", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("actor_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("actor_login", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("actor_display_login", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("actor_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("actor_avatar_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("repo_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("repo_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("repo_url", "STRING", mode="NULLABLE")
    ]
    clustering_fields = ["id", "field", "public", "repo_name"]
    time_partitoning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="created_at",  # name of column to use for partitioning
        expiration_ms=7776000000
    )  # 90 days

    # create table
    # create_table(
    #     gbq_table_name=table_name,
    #     schema=schema,
    #     clustering_fields=clustering_fields,
    #     time_partitioning=time_partitoning
    # )

    # write data to table
    write_bq(df, table_name)

    if teardown:
        tear_down()


@flow(
    name="Main-BQ-Flow"
)
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
    main_bq_flow(year=2020, month=1, days=[1], hours=[1])
