from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from utils import rename_cols
import requests

@task(retries=3)
def fetch(url, filename) -> pd.DataFrame:
    """Read data from https://www.gharchive.org/ into pandas DataFrame"""
    path = f"data/raw/{filename}.json.gzip"
    get_response = requests.get(url, stream=True)
    with open(path, 'wb') as f:
        for chunk in get_response.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
    
    df = pd.read_json(path, compression='gzip', lines=True)
    return df

@task(retries=3)
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    
    df.drop(["payload", "org"], axis=1, inplace=True)
    
    actor = pd.json_normalize(df["actor"])
    actor = actor.drop("gravatar_id", axis=1)
    rename_cols(actor, "actor")
    
    repo = pd.json_normalize(df["repo"])
    rename_cols(repo, "repo")
    
    clean_data = df.join([actor, repo])
    clean_data.drop(["actor", "repo"], axis=1, inplace=True)

    return clean_data
    
@task()
def write_local(df: pd.DataFrame, filename: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/clean/{filename}.csv") 
    df.to_csv(path)
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)

@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    Path("./data/raw").mkdir(parents=True, exist_ok=True)
    Path("./data/clean").mkdir(parents=True, exist_ok=True)
    
    year = 2015
    month = 1
    date = 1
    hour = 15
    filename = f"{year}-{month:02}-{date:02}-{hour:02}"
    url = f"https://data.gharchive.org/{filename}.json.gz"

    df = fetch(url, filename)
    clean_df = transform_data(df)
    path = write_local(clean_df, filename)
    print(path)


if __name__ == "__main__":
    etl_web_to_gcs()
