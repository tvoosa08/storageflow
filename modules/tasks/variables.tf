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

variable "module_unique_prefix" {
  description = "Unique prefix for the module resources"
  type        = string
}


variable "task_queue_region" {
  description = "Region for the Cloud Tasks queue"
  type        = string
}

variable "task_queue_name" {
  description = "Name of the Cloud Tasks queue"
  type        = string
}


