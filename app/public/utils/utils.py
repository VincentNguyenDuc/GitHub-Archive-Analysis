import streamlit as st
import pandas as pd
from google.cloud.bigquery import Client
from google.oauth2 import service_account
from utils.constants import GcpConstants


@st.cache_data(ttl=600)
def run_query(query: str) -> pd.DataFrame:
    gcp_creds = service_account \
        .Credentials \
        .from_service_account_info(
            st.secrets["gcp_service_account"]
        )

    client = Client(
        project=GcpConstants.PROJECT_ID,
        credentials=gcp_creds,
        location=GcpConstants.LOCATION
    )
    query_job = client.query(
        query=query,
        location=GcpConstants.LOCATION,
        project=GcpConstants.PROJECT_ID
    )
    return query_job.to_dataframe()
