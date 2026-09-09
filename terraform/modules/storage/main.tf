resource "google_storage_bucket" "this" {
  name          = var.bucket_name
  location      = var.location
  storage_class = var.storage_class

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  soft_delete_policy {
    retention_duration_seconds = 604800
  }

  encryption {
    google_managed_encryption_enforcement_config {
      restriction_mode = "NotRestricted"
    }

    customer_managed_encryption_enforcement_config {
      restriction_mode = "NotRestricted"
    }

    customer_supplied_encryption_enforcement_config {
      restriction_mode = "FullyRestricted"
    }
  }
}