variable "service_account" {
  description = "Service account to use for the storageflow service"
  type        = string
}

variable "service_account_id" {
  description = "Service account ID for the storageflow service"
  type        = string
}

variable "project_id" {
  description = "GCP project ID where the storageflow service will be deployed"
  type        = string
}

variable "region" {
  description = "GCP region where the storageflow service will be deployed"
  type        = string
}

variable "config_path" {
  description = "Path to the configuration files for StorageFlow"
  type        = string
}

variable "config_included_patterns" {
  description = "Patterns to include in the configuration files"
  type        = list(string)
}

variable "config_excluded_patterns" {
  description = "Patterns to exclude in the configuration files"
  type        = list(string)
}

variable "gcs_source_staging_dir" {
  description = "GCS staging directory for source code"
  type        = string
}

variable "module_unique_prefix" {
  description = "Unique prefix for the module resources"
  type        = string
}

variable "gce_subnetwork_uri" {
  description = "Subnetwork URI for the Dataproc cluster"
  type        = string
}

variable "gce_tags" {
  description = "Tags for the GCE instances"
  type        = list(string)
}


variable "task_queue_region" {
  description = "Region for the task queue"
  type        = string
}

variable "artifact_registry_repo_id" {
  description = "ID of the Artifact Registry repository"
  type        = string
}


variable "artifact_registry_repo_location" {
  description = "Location of the Artifact Registry repository"
  type        = string
}

variable "artifact_registry_repo_name" {
  description = "Name of the Artifact Registry repository"
  type        = string
}

variable "network_name" {
  description = "Name of the network to use for the Cloud Run service"
  type        = string
}

variable "subnetwork_name" {
  description = "Name of the subnetwork to use for the Cloud Run service"
  type        = string
}

variable "environment" {
  description = "Environment for the service (e.g., prod, dev)"
  type        = string
}


variable "gcs_temp_bucket" {
  description = "Temporary GCS bucket for the service"
  type        = string
}

variable "logging_level" {
  description = "Logging level for the service"
  type        = string
}

variable "firestore_location_id" {
  description = "Location ID for Firestore database"
  type        = string
}

variable "firestore_database_name" {
  description = "Firestore database name"
  type        = string
  default     = "storageflow"
}
