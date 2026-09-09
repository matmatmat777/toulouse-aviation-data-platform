resource "google_bigquery_dataset_iam_member" "raw_loader_writer" {
  project    = var.project_id
  dataset_id = module.bigquery_raw.dataset_id

  role   = "roles/bigquery.dataEditor"
  member = "serviceAccount:${google_service_account.bigquery_loader.email}"
}