
# Define the Google Cloud Workflow
resource "google_workflows_workflow" "w" {
  name            = var.workflow_name
  region          = var.workflow_location
  description     = "Storageflow Controller Workflow"
  service_account = var.service_account

  # Read the workflow content from the local YAML file
  source_contents = templatefile("${path.module}/storageflow_controller.yaml", {
    cloud_run_base_url = var.service_url
  })

  labels = {
    module     = var.module_id
    managed_by = "terraform"
  }

}

