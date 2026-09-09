resource "google_service_account" "bigquery_loader" {
  account_id   = "aviation-prod-bigquery-loader"
  display_name = "aviation-prod-bigquery-loader"
  description  = "Loads production raw aircraft position data into BigQuery"
}