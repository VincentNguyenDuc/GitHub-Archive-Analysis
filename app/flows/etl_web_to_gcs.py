from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from utils import rename_cols
import requests

@task(retries=3)
def fetch(url:str, filename:str) -> pd.DataFrame:
    """
    Read data from https://www.gharchive.org/ into pandas DataFrame

    Args:
        url (str): The URL of the data source
        filename (str): dataset name

    Returns:
        pd.DataFrame: a pandas dataframe
    """
    path = f"./tmp/{filename}.json.gzip"
    print("\n" + filename + "\n")
    get_response = requests.get(url, stream=True)
    with open(path, 'wb') as f:
        for chunk in get_response.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
    
    df = pd.read_json(path, compression='gzip', lines=True)
    return df

@task(retries=3)
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform a simple data wrangling

    Args:
        df (pd.DataFrame): a dataframe

    Returns:
        pd.DataFrame: clean dataframe
    """
    
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
    """Write DataFrame out locally as csv gzipped file

    Args:
        df (pd.DataFrame): a dataframe
        filename (str): the dataset file name

    Returns:
        Path: a path object point at the location of the writen file
    """
    path = Path(f"./tmp/{filename}.csv.gz") 
    df.to_csv(path, compression="gzip")
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload local file to GCS

    Args:
        path (Path): the path of the file
    """
    gcs_block = GcsBucket.load("github-archive-gcs")
    gcs_block.upload_from_path(
        from_path=path,
        to_path=f"2015/{path.name}"
    )

@flow(log_prints=True)
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    Path("./tmp").mkdir(parents=True, exist_ok=True)
    
    year = 2015
    month = 1
    date = 1
    hour = 15
    filename = f"{year}-{month:02}-{date:02}-{hour:02}"
    url = f"https://data.gharchive.org/{filename}.json.gz"

    df = fetch(url, filename)
    clean_df = transform_data(df)
    path = write_local(clean_df, filename)
    write_gcs(path)

if __name__ == "__main__":
    etl_web_to_gcs()
