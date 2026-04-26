# Create a custom IAM role with refined permissions
resource "google_project_iam_custom_role" "runtime_role" {
  project     = var.project_id
  role_id     = var.runtime_role_name
  title       = "Storageflow Runtime Role"
  description = "Custom role for a service account focusing on runtime operations with granular permissions, avoiding broad admin roles."
  permissions = var.runtime_role_permissions
  stage       = "GA"
}

# Grant the refined custom role to the service account
#resource "google_project_iam_member" "grant_role" {
#  project = var.project_id
#  role    = google_project_iam_custom_role.runtime_role.name
#  member  = "serviceAccount:${var.service_account}"
#}

#output "custom_role_name_refined" {
#  description = "The name of the created refined custom IAM role."
#  value       = google_project_iam_custom_role.runtime_role.name
#}

