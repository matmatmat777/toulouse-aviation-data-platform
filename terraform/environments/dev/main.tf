terraform {
  required_version = ">= 1.16.0"

  backend "gcs" {
    bucket = "toulouse-aviation-tfstate-408818015704"
    prefix = "terraform/dev"
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
  dataset_id = "aviation_raw"
  location   = "EUROPE-WEST9"
}

module "processed_storage" {
  source = "../../modules/storage"

  bucket_name = "toulouse-aviation-data-processed"
  location    = var.region
}

module "rejected_data" {
  source = "../../modules/storage"

  bucket_name = "toulouse-aviation-data-rejected"
  location    = var.region
}

