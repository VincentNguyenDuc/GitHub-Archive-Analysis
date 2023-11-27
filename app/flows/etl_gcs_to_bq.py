from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import pandas as pd
import requests

@flow()
def etl_gcs_to_bq():
    """Load data from Google Cloud Storage to BigQuery"""
    pass
