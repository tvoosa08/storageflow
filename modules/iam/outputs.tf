output "runtime_role_id" {
  description = "The ID of the created refined custom IAM role."
  value       = google_project_iam_custom_role.runtime_role.name
}
