"""
Serve a deployment to Prefect Cloud
"""

from prefect import flow
from etl_gcs_to_bq import main_bq_flow
from etl_web_to_gcs import main_gcs_flow
from datetime import datetime


@flow(
    name="GCP-Flow",
    retries=3,
    log_prints=True
)
def gcp_flow(
    year: int,
    month: int = 1,
    days: list[int] = None,
    hours: list[int] = None
):
    """The Main Flow for Deployment:
    - Data Source to GCS
    - GCS to BQ

    Args:
        year (int, optional): a year.
        month (int, optional): a month. Defaults to 1.
        days (list[int], optional): a list of days. Defaults to (1 - 31)
        hours (list[int], optional): a list of hours. Defaults to (0 - 23)
    """
    if days is None:
        days = list(range(1, 32))
    if hours is None:
        hours = list(range(24))
    main_gcs_flow(year, month, days, hours)
    main_bq_flow(year, month, days, hours)


if __name__ == "__main__":
    dt = datetime(2020, 1, 1)
    gcp_flow.serve(
        name=f"GCP-Flow-Deployment-({dt.strftime('%b-%Y')})",
        parameters={
            "year": dt.year,
            "month": dt.month,
            "days": list(range(2, 32))
        }
    )
