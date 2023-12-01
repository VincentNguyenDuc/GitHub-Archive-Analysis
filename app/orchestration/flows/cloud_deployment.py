"""
Serve a deployment to Prefect Cloud
"""

from prefect import flow
from etl_gcs_to_bq import main_bq_flow
from etl_web_to_gcs import main_gcs_flow
from argparse import ArgumentParser


@flow(
    name="GCP-Flow",
    retries=3,
    log_prints=True
)
def gcp_flow(
    flowname: str,
    year: int,
    month: int = 1
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
    days = list(range(1, 32))
    hours = list(range(24))

    if flowname == "GCS":
        main_gcs_flow(year, month, days, hours)
    elif flowname == "BQ":
        main_bq_flow(year, month, days, hours)
    elif flowname == "ALL":
        main_gcs_flow(year, month, days, hours)
        main_bq_flow(year, month, days, hours)
    else:
        raise KeyError("This should not happen!")


if __name__ == "__main__":

    parser = ArgumentParser(description="Deployment Configuration")
    parser.add_argument("-f", "--flowname",
                        choices=["GCS", "BQ", "ALL"], required=True, type=str)
    parser.add_argument(
        "-y", "--year", choices=list(range(2015, 2023)), default=2020, type=int)
    parser.add_argument(
        "-m", "--month", choices=list(range(1, 13)), default=1, type=int)
    args = parser.parse_args()

    flowname, year, month = args.flowname, args.year, args.month
    gcp_flow.serve(
        name=f"GCP-{flowname}-Deployment-({year - month})",
        parameters={
            "name": flowname,
            "year": year,
            "month": month
        }
    )
