# Prefect Flows
Workflows from data source to Google Cloud Platform

# Commands

## Manually run the flows using python
```bash
# Data Source to GCS
python3 ${FLOWS_DIR}/etl_web_to_gcs.py

# GCS to BQ
python3 ${FLOWS_DIR}/etl_gcs_to_bq.py

# Deployment
python3 ${FLOWS_DIR}/cloud_deployment.py
```

## Docker

```bash
# Build the image
docker image build -t vincent1601/gharchive-flows:{tag} .

# Push to DockerHub
docker push vincent1601/gharchive-flows:{tag}

# Deploy
docker run -e PREFECT_API_URL=YOUR_PREFECT_API_URL -e PREFECT_API_KEY=YOUR_API_KEY vincent1601/gharchive-flows:{tag}
```