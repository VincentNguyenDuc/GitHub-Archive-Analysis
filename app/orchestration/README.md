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
python3 cloud_deployment.py
usage: cloud_deployment.py [-h] -f {GCS,BQ,ALL} [-y {2015,2016,2017,2018,2019,2020,2021,2022}]
                           [-m {1,2,3,4,5,6,7,8,9,10,11,12}]

options:
  -h, --help            show this help message and exit
  -f {GCS,BQ,ALL}, --flowname {GCS,BQ,ALL}
  -y {2015,2016,2017,2018,2019,2020,2021,2022}, --year {2015,2016,2017,2018,2019,2020,2021,2022}
  -m {1,2,3,4,5,6,7,8,9,10,11,12}, --month {1,2,3,4,5,6,7,8,9,10,11,12}
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