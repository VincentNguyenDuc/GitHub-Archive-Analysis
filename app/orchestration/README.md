# Prefect Flows
Workflows from data source to Google Cloud Platform

# Commands

## Manually run the flows using python
```bash
# Data Source to GCS
python3 ${FLOWS_DIR}/etl_web_to_gcs.py

# GCS to BQ
python3 ${FLOWS_DIR}/etl_gcs_to_bq.py
```

## Docker

- Push a docker image of Flows code and requirements to DockerHub
- Use that remote image to create a Prefect Block
- Use that Prefect Block to deploy

```bash
# Build the image
docker image build -t vincent1601/gharchive-flows:v001 .

# Push to DockerHub
docker push vincent1601/gharchive-flows:v001

# Start Prefect agent
prefect agent start  --work-queue "default"
```