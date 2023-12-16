import matplotlib.pyplot as plt
import streamlit as st
from utils.utils import run_query
from utils.constants import GcpConstants, DataConstants


st.set_page_config(
    page_title="GitHub Archive Dashboard",
    page_icon="random",
    layout="wide",
    initial_sidebar_state="collapsed"
)

################################# GitHub Actions Types #################################

st.title("Breaking Down Common GitHub Actions")

types_occurences_df = run_query(f"""
    SELECT
        table_name action,
        total_rows total
    FROM
        `region-{GcpConstants.LOCATION}`.INFORMATION_SCHEMA.TABLE_STORAGE
    WHERE
        total_rows > 0
    ORDER BY
        total_rows
""")

col1, col2 = st.columns([1, 3])
with col1:
    st.write(types_occurences_df.set_index("action"))

with col2:
    # Pie chart, where the slices will be ordered and plotted counter-clockwise:
    labels = types_occurences_df["action"]
    sizes = types_occurences_df["total"]

    fig, ax = plt.subplots()
    ax.set_title("Breakdown of GitHub Actions")
    ax.pie(
        sizes,
        startangle=90,
        autopct=lambda p: f"{p:.2f}%" if p >= 2 else None,
        textprops={'fontsize': 4}
    )
    ax.axis('equal')
    ax.legend(labels=labels, loc="lower right", fontsize=5)

    st.pyplot(fig)


################################# Actions By Hour #################################
st.title("GitHub Action by Hours")

option = st.selectbox(
    label="Please Select a GitHub Action:",
    options=DataConstants.GITHUB_EVENTS
)
hour_occurences_df = run_query(
    f"""
    select 
        extract(hour from created_at) hour,
        count(*) total
    from `{GcpConstants.PROJECT_ID}.{GcpConstants.BQ_DATASET}.{option}`
    group by hour
    order by hour
    """
)
col1, col2 = st.columns([3, 1])
with col1:
    st.bar_chart(
        data=hour_occurences_df,
        x="hour",
        y="total",
        color="#1799dd"
    )
with col2:
    st.write(hour_occurences_df.set_index("hour"))
