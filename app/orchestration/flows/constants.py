class GcpConstants:
    FILE_EXTENSION = "csv.gz"
    BUCKET_NAME = "github-archive-gcs"
    CREDS_NAME = "github-archive-gcp-creds"
    BQ_DATASET = "github_archive_data_all"
    PROJECT_ID = "friendly-basis-406112"

class PrefectConstants:
    DOCKER_BLOCK = "github-archive-docker"


class LocalConstants:
    TEMP_PATH = "./tmp"


class DataConstants:
    FILE_EXTENSION = "json.gz"
    COMPRESSION_TYPE = "gzip"
    SOURCE_URL = "https://data.gharchive.org"
