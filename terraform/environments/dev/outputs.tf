output "raw_bucket_name" {
  description = "Name of the RAW GCS bucket"
  value       = module.storage.bucket_name
}

output "raw_bucket_url" {
  description = "GCS URL of the RAW bucket"
  value       = module.storage.bucket_url
}

output "processed_bucket_url" {
  description = "GCS URL of the processed bucket"
  value       = module.processed_storage.bucket_url
}

output "rejected_data_url" {
  description = "GCS URL of the rejected data"
  value       = module.rejected_data.bucket_url
}






