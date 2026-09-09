terraform {
  required_version = ">= 1.16.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
  }
}

provider "google" {
  project = "toulouse-aviation-data"
  region  = "europe-west9"
}

resource "google_storage_bucket" "terraform_state" {
  name          = "toulouse-aviation-tfstate-408818015704"
  location      = "EUROPE-WEST9"
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }

  soft_delete_policy {
    retention_duration_seconds = 604800
  }

  lifecycle {
    prevent_destroy = true
  }
}