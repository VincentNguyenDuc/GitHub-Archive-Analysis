"""
Constants Variables
"""

class GcpConstants:
    """
    Additional Information for Google Cloud Platform
    """
    FILE_EXTENSION = "csv.gz"
    BUCKET_NAME = "github-archive-gcs"
    CREDS_NAME = "github-archive-gcp-creds"
    BQ_DATASET = "github_archive_data_all"
    PROJECT_ID = "friendly-basis-406112"
    LOCATION = "asia-east2"


class PrefectConstants:
    """
    Additional Information for Prefect
    """
    DOCKER_BLOCK = "github-archive-docker"


class LocalConstants:
    """
    Additional Information for Environment
    """
    TEMP_PATH = "./tmp"


class DataConstants:
    """
    Adidtional Information for Data
    """
    FILE_EXTENSION = "json.gz"
    COMPRESSION_TYPE = "gzip"
    SOURCE_URL = "https://data.gharchive.org"
