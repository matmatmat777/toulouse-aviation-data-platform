resource "google_bigquery_dataset" "this" {
  dataset_id = var.dataset_id
  project    = var.project_id
  location   = var.location
}