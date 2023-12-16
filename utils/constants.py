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
    GCP_SERVICE_ACCOUNT = "gcp_service_account"
    

class DataConstants:
    """
    Adidtional Information for Data
    """
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
    