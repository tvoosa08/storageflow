variable "project_id" {
  description = "The ID of the project in which to create the Firestore database."
  type        = string
}

variable "firestore_database_name" {
  description = "The Firestore database ID."
  type        = string
  default     = "storageflow"
}

variable "dedup_collection" {
  description = "The name of the collection used for deduplication."
  type        = string
  default     = "storageflow_core_dedup"
}

variable "firestore_location_id" {
  description = "The region to create the Firestore database in."
  type        = string
}
