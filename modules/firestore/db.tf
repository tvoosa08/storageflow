resource "google_firestore_database" "database" {
  project     = var.project_id
  name        = var.firestore_database_name
  location_id = var.firestore_location_id
  type        = "FIRESTORE_NATIVE"
}

resource "google_firestore_index" "task_key_created_at_index" {
  project    = var.project_id
  database   = var.firestore_database_name
  collection = var.dedup_collection

  fields {
    field_path = "task_key"
    order      = "ASCENDING"
  }

  fields {
    field_path = "created_at"
    order      = "DESCENDING"
  }
  depends_on = [google_firestore_database.database]
}
