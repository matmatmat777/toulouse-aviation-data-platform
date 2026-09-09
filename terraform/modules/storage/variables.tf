variable "bucket_name" {
  description = "Name of the GCS bucket"
  type        = string
}

variable "location" {
  description = "GCS bucket location"
  type        = string
}

variable "storage_class" {
  description = "Storage class name"
  type        = string
  default     = "STANDARD"
}