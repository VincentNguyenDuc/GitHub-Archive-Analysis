# Commands
This is where we initialize the infrastructure in Google Cloud Platform
- Data Lake: Google Cloud Storage (GCS)
- Data Warehouse: Google BigQuery (BQ)

After constructing the infrastructure, we will use "Prefect" to create data pipelines (flows) from data source to GCS, and from GCS to BQ.

```bash
# Refresh service-account's auth-token for this session
gcloud auth application-default login

# Initialize state file (.tfstate)
terraform init

# Check changes to new infra plan
terraform plan
```

```bash
# Create new infra
terraform apply
```

```bash
# Delete infra after your work, to avoid costs on any running services
terraform destroy
```