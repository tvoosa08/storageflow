locals {
  module_id            = "storageflow"
  service_name         = "${var.module_unique_prefix}-${local.module_id}-service"
  task_queue_name      = "${var.module_unique_prefix}-${local.module_id}-tasks"
  module_unique_prefix = "${var.module_unique_prefix}-${local.module_id}"
  workflow_name        = "${var.module_unique_prefix}-${local.module_id}-controller"
}



module "cloud_run" {
  source = "../cloud_run"

  module_unique_prefix = local.module_unique_prefix
  module_id            = local.module_id
  project_id           = var.project_id
  region               = var.region

  service_account    = var.service_account
  service_account_id = var.service_account_id

  gcs_source_staging_dir = var.gcs_source_staging_dir

  artifact_registry_repo_id       = var.artifact_registry_repo_name
  artifact_registry_repo_location = var.artifact_registry_repo_location


  config_path              = var.config_path
  config_included_patterns = var.config_included_patterns
  config_excluded_patterns = var.config_excluded_patterns

  gce_subnetwork_uri = var.gce_subnetwork_uri
  gce_tags           = var.gce_tags

  task_queue_region = var.task_queue_region
  task_queue_name   = local.task_queue_name

  network_name    = var.network_name
  subnetwork_name = var.subnetwork_name

  environment     = var.environment
  gcs_temp_bucket = var.gcs_temp_bucket

  logging_level = var.logging_level

  workflow_name = local.workflow_name

  workflow_location = var.region

  firestore_database_name = var.firestore_database_name
}

module "task_queue" {
  source               = "../tasks"
  service_url          = module.cloud_run.service_url
  service_account      = var.service_account
  service_audience     = module.cloud_run.service_url
  project_id           = var.project_id
  module_unique_prefix = local.module_unique_prefix

  task_queue_name   = local.task_queue_name
  task_queue_region = var.task_queue_region
}


module "workflow" {
  source            = "../workflows"
  service_url       = module.cloud_run.service_url
  service_account   = var.service_account
  project_id        = var.project_id
  workflow_location = var.region
  workflow_name     = local.workflow_name
  module_id         = local.module_id
  service_audience  = module.cloud_run.service_url
}


module "firestore" {
  source                  = "../firestore"
  firestore_location_id   = var.firestore_location_id
  firestore_database_name = var.firestore_database_name
  project_id              = var.project_id

}

