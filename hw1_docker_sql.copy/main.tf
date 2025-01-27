
# configurate GCP provider
provider "google" {
  project = "data-engineer-zoomcamp-448404" 
  region  = "us-west2"
}

# create GCP Storage Bucket
resource "google_storage_bucket" "example_bucket" {
  name     = "data-engineer-zoomcamp-bucket-20250127"
  location = "US"
}

# create BigQuery Dataset
resource "google_bigquery_dataset" "example_dataset" {
  dataset_id = "green_taxi_trips_dataset"
  location   = "US"
}
