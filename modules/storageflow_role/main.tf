module "iam" {
  source            = "../iam"
  project_id        = var.project_id
  runtime_role_name = var.runtime_role_name
}
