resource "google_service_account" "bigquery_loader" {
  account_id   = "aviation-bigquery-loader"
  display_name = "aviation-bigquery-loader"
  description  = "Loads raw aircraft position data from GCS into BigQuery"
}