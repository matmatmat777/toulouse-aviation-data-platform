terraform {
  required_version = ">= 1.16.0"

  backend "gcs" {
    bucket = "toulouse-aviation-tfstate-408818015704"
    prefix = "terraform/prod"
  }

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

module "storage" {
  source = "../../modules/storage"

  bucket_name = local.raw_bucket_name
  location    = var.region
}

module "bigquery_raw" {
  source = "../../modules/bigquery"

  project_id = var.project_id
  dataset_id = "aviation_prod_raw"
  location   = "EUROPE-WEST9"
}