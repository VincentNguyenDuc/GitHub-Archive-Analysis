from prefect import flow
from etl_gcs_to_bq import main_bq_flow
from etl_web_to_gcs import main_gcs_flow


@flow(log_prints=True, retries=3)
def gcp_flow(
    year: int = 2015,
    month: int = 1,
    days: list[int] = None,
    hours: list[int] = None
):
    if days is None:
        days = list(range(1, 32))
    if hours is None:
        hours = list(range(24))
    main_gcs_flow(year, month, days, hours)
    main_bq_flow(year, month, days, hours)


if __name__ == "__main__":
    gcp_flow.serve(
        name="GCP-Flow-Deployment-2015",
        parameters={
            "year": 2016,
            "month": 1,
            "days": [
                1
            ],
        }
    )
