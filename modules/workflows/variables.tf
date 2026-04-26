variable "service_url" {
  description = "URL of the service to be used by the tasks"
  type        = string
}

variable "service_account" {
  description = "Service account to use for the storageflow service"
  type        = string
}

variable "service_audience" {
  description = "Audience for the service account used in Cloud Tasks"
  type        = string
}

variable "project_id" {
  description = "GCP project ID where the storageflow service will be deployed"
  type        = string
}

variable "workflow_location" {
  description = "GCP region where the storageflow service will be deployed"
  type        = string
}

variable "workflow_name" {
  description = "Name of the Google Cloud Workflow"
  type        = string
}

variable "module_id" {
  description = "Unique identifier for the module"
  type        = string
}

