import matplotlib.pyplot as plt
import streamlit as st
from utils.utils import run_query
from utils.constants import GcpConstants


st.set_page_config(
    page_title="GitHub Archive Dashboard",
    page_icon="random",
    layout="wide",
    initial_sidebar_state="collapsed"
)

################################# Actions By Hour #################################
st.title("Number of GitHub Actions by Hours")
hour_occurences_df = run_query(
    f"""
    select 
        extract(hour from created_at) hour,
        count(*) total_actions
    from `{GcpConstants.PROJECT_ID}.{GcpConstants.BQ_DATASET}.2020`
    group by hour
    order by hour
    """
)
col1, col2 = st.columns([3, 1])
with col1:
    st.bar_chart(
        data=hour_occurences_df,
        x="hour",
        y="total_actions",
        color="#1799dd"
    )
with col2:
    st.write(hour_occurences_df.set_index("hour"))


################################# GitHub Actions Types #################################

st.title("Breaking Down Common GitHub Actions")

types_occurences_df = run_query(
    f"""
    select 
        type,
        count(*) total_actions
    from `{GcpConstants.PROJECT_ID}.{GcpConstants.BQ_DATASET}.2020`
    group by type
    order by total_actions
    """
)

col1, col2 = st.columns([1, 3])
with col1:
    st.write(types_occurences_df.set_index("type"))

with col2:
    # Pie chart, where the slices will be ordered and plotted counter-clockwise:
    labels = types_occurences_df["type"]
    sizes = types_occurences_df["total_actions"]

    fig, ax = plt.subplots()
    ax.set_title("Breakdown of GitHub Actions Types")
    ax.pie(
        sizes,
        startangle=90,
        autopct=lambda p: f"{p:.2f}%" if p >= 2 else None,
        textprops={'fontsize': 4}
    )
    ax.axis('equal')
    ax.legend(labels=labels, loc="lower right", fontsize=5)

    st.pyplot(fig)
