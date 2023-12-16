import os
from constants import DataConstants

for event in DataConstants.GITHUB_EVENTS:
    os.system(
        f"gzip -d ./tmp/{event}/*.gz"
    )
    os.system(
        f"generate-schema < ./tmp/{event}/2020-01-01-1.json > ./schemas/{event}.json"
    )
