"""
Constants Variables
"""


class GcpConstants:
    """
    Additional Information for Google Cloud Platform
    """
    BUCKET_NAME = "github-archive-gcs"
    CREDS_NAME = "github-archive-gcp-creds"
    BQ_DATASET = "github_archive_data_all"
    PROJECT_ID = "friendly-basis-406112"
    LOCATION = "asia-east2"
    URI_ROOT = f"https://storage.cloud.google.com/github_archive_data_lake_{PROJECT_ID}"


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
    GITHUB_EVENTS = [
        "CommitCommentEvent",
        "CreateEvent",
        "DeleteEvent",
        "ForkEvent",
        "GollumEvent",
        "IssueCommentEvent",
        "IssuesEvent",
        "MemberEvent",
        "PublicEvent",
        "PullRequestEvent",
        "PullRequestReviewCommentEvent",
        "PushEvent",
        "ReleaseEvent",
        "WatchEvent"
    ]
