"""
Orchestrated the data from Google Cloud Storage to Google BigQuery.
- Get the data from GCS, which have already been separated by types of GitHub events.
- Write the data to different BigQuery tables.
- Each table might have differences in schema
"""

from constants import GcpConstants, DataConstants
from prefect import flow
from prefect_gcp.bigquery import TimePartitioning, LoadJobConfig
from prefect_gcp import GcpCredentials
from google.cloud.bigquery import SourceFormat
from datetime import datetime
from utils import get_schema


gcp_creds: GcpCredentials = GcpCredentials.load(GcpConstants.CREDS_NAME)
client = gcp_creds.get_bigquery_client(
    project=GcpConstants.PROJECT_ID,
    location=GcpConstants.LOCATION
)


@flow(name="GCS-To-BQ")
def etl_gcs_to_bq(
    event_name: str,
    dt: datetime
) -> None:
    """Load data from Google Cloud Storage to BigQuery

    Args:
        event_name (str): the name of the github event
        dt (datetime): a datetime object with (year, month, day, hour)
    """
    table_id = f"{GcpConstants.PROJECT_ID}.{GcpConstants.BQ_DATASET}.{event_name}"

    job_config = LoadJobConfig(
        schema=get_schema(event_name),
        source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
        time_partitioning=TimePartitioning(field="created_at"),
        clustering_fields=["created_at"],
        autodetect=True
    )

    year, month, day, hour = dt.year, dt.month, dt.day, dt.hour
    file_path = f"{event_name}/{year}-{month:02}-{day:02}-{hour}.{DataConstants.FILE_EXTENSION}"
    uri = f"{GcpConstants.URI_ROOT}/{file_path}"

    load_job = client.load_table_from_uri(
        source_uris=uri,
        destination=table_id,
        location=GcpConstants.LOCATION,
        job_config=job_config
    )

    load_job.result()  # Waits for the job to complete.


if __name__ == "__main__":
    for event_name in DataConstants.GITHUB_EVENTS:
        for h in range(1, 24):
            dt = datetime(year=2020, month=1, day=1, hour=h)
            etl_gcs_to_bq(event_name, dt)
