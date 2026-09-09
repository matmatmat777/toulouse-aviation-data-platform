output "raw_bucket_name" {
  description = "Name of the RAW GCS bucket"
  value       = module.storage.bucket_name
}

output "raw_bucket_url" {
  description = "GCS URL of the RAW bucket"
  value       = module.storage.bucket_url
}