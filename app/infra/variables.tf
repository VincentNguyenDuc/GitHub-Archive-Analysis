locals {
  data_lake_bucket = "github_archive_data_lake"
}

variable "project" {
  description = "Your GCP Project ID. Change as per your project ID"
  default     = "friendly-basis-406112"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "asia-east2"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
  default     = "github_archive_data_all"
}
