variable "project_id" {
  description = "GCP project ID where the storageflow service will be deployed"
  type        = string
}

variable "runtime_role_name" {
  description = "The name of the created refined custom IAM role."
  type        = string
}

