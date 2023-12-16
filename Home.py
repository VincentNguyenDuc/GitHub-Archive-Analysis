import streamlit as st
from streamlit_lottie import st_lottie
from utils.utils import load_lottieurl

st.set_page_config(
    page_title="GitHub Archive Greetings",
    page_icon="random",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st_lottie(
    load_lottieurl(
        "https://lottie.host/23eb30b6-013f-4c68-9847-8bf18fefb602/F5YuqzIyI5.json"
    ),
    speed=2,
    quality="low",
    height=300,
    key="initial"
)

st.title("Greetings:wave:")
st.markdown("""
    Greetings everyone, I'm Vincent Nguyen, and I'm excited to share my first end-to-end Data Engineering project with you.
    You can access the source code at [GitHub](https://github.com/VincentNguyenDuc/GitHub-Archive-Analysis).
    Your contributions and feedback are highly valued and greatly appreciated.
    Feel free to raise any issues or contribute to make this project better!
""")

st.title("Data Source:smiling_imp:")
st.markdown("""
    Open-source developers all over the world are working on millions of projects: writing code & documentation, fixing & submitting bugs, and so forth.
    [GH Archive](https://www.gharchive.org/) is a project to record the public GitHub timeline, archive it, and make it easily accessible for further analysis.
""")

st.title("This project:fire:")
st.markdown("""
    - Utilize Terraform to build the GCP infrastructure, incorporating Google Cloud Storage (GCS) and BigQuery (BQ).
    - Employ Prefect to orchestrate the transfer of JSON files from the GitHub API to GCS and subsequently from GCS to BQ.
    - Perform data wrangling and analysis to gain insights into user behaviors on GitHub.
    - Build simple Machine Learning models with BigQuery to predict GitHub's user behaviours.
    - Thoroughly document and detail each step involved in the entire process.
    - Create an engaging and informative Streamlit web application to visually present the results.
""")

st.title("Going Further:rocket:")
st.markdown("""
    Check out my [Personal Portfolio](https://vincentnguyenduc.github.io/)
    to explore more about other projects that I'm working on!
""")
