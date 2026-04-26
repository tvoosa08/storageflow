locals {
  image_name          = "${var.module_unique_prefix}-runtime-image"
  service_name        = "${var.module_unique_prefix}-service"
  image_tag           = "latest"
  src_folder          = path.module
  docker_context_root = "./tmp"
  docker_file         = "./Dockerfile"
}


module "deps" {
  source = "../helpers/dependency_collector"
  dependencies = [
    {
      name              = "function"
      source_path       = "${path.module}/../../src"
      included_patterns = ["**"]
      excluded_patterns = ["**/__pycache__/"]
    },
    {
      name              = "function_installer"
      source_path       = "${path.module}/../../"
      included_patterns = ["poetry.lock", "pyproject.toml", "README.md"]
      excluded_patterns = []
    },
    {
      name              = "config"
      source_path       = var.config_path
      included_patterns = var.config_included_patterns
      excluded_patterns = var.config_excluded_patterns
      target_sub_dir    = "config/user"
    },
    {
      name              = "root_config"
      source_path       = "${path.module}/../../root_config"
      included_patterns = ["**/*.toml", "**/*.py"]
      excluded_patterns = ["**/__pycache__/"]
      target_sub_dir    = "config/root"
    },
    {
      name              = "dockerfile"
      source_path       = path.module,
      included_patterns = ["**/Dockefile", "cloud_run.tf"]
      excluded_patterns = ["**/__pycache__/"]
    },
  ]
}


resource "null_resource" "redeploy_code" {
  triggers = {
    source_code_hash = module.deps.source_hash
  }
  depends_on = [module.deps]
}

data "google_artifact_registry_repository" "repo" {
  location      = var.artifact_registry_repo_location
  repository_id = var.artifact_registry_repo_id
}


resource "null_resource" "build_image" {

  triggers = {
    source_code_hash = module.deps.source_hash
  }

  provisioner "local-exec" {
    command = <<-EOT
      echo "Collecting dependencies"
      rm -rf ${path.module}/tmp
      mkdir -p ${path.module}/tmp
      cp -R ${module.deps.staging_dir}/* ${path.module}/tmp/
    EOT
  }

  provisioner "local-exec" {
    working_dir = "${local.src_folder}/"
    command     = <<-EOT
      gcloud builds submit \
      --config cloudbuild.yaml \
      --substitutions '_REPO_NAME=${var.artifact_registry_repo_id},_REPO_REGION=${var.region},_IMAGE_TAG=${local.image_tag},_IMAGE_NAME=${local.image_name},_SERVICE_ACCOUNT=${var.service_account_id},_DOCKER_FILE=${local.docker_file}' \
      --gcs-source-staging-dir="gs://${var.gcs_source_staging_dir}/${var.module_unique_prefix}/deploy/" 
    EOT
  }

  depends_on = [null_resource.redeploy_code]

  lifecycle {
    replace_triggered_by = [null_resource.redeploy_code]
  }
}


data "google_artifact_registry_docker_image" "image" {
  location      = data.google_artifact_registry_repository.repo.location
  repository_id = data.google_artifact_registry_repository.repo.repository_id
  image_name    = "${local.image_name}:${local.image_tag}"

  depends_on = [null_resource.build_image]
}



resource "google_cloud_run_v2_service" "api_service" {
  name                = local.service_name
  deletion_protection = false
  location            = var.region
  client              = "terraform"
  #ingress             = "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER"
  #ingress = "INGRESS_TRAFFIC_ALL"
  ingress = "INGRESS_TRAFFIC_INTERNAL_ONLY"
  template {
    execution_environment            = "EXECUTION_ENVIRONMENT_GEN2"
    max_instance_request_concurrency = 2
    containers {
      image = data.google_artifact_registry_docker_image.image.self_link
      # Startup probe of application within the container. All other probes are disabled if a startup
      # probe is provided, until it succeeds. Container will not be added to service endpoints if the
      # probe fails.
      # More info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#container-probes
      startup_probe {
        http_get {
          path = "/ping"
          port = 8080
        }
        initial_delay_seconds = 60
        period_seconds        = 120
        timeout_seconds       = 15
        failure_threshold     = 45
      }
      # Periodic probe of container liveness. Container will be restarted if the probe fails.
      # More info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#container-probes
      liveness_probe {
        http_get {
          path = "/ping"
          port = 8080
        }
        timeout_seconds   = 300
        period_seconds    = 120
        failure_threshold = 5
      }

      resources {
        limits = {
          cpu    = "8"
          memory = "16Gi"
        }
      }

      env {
        name  = "APP_ENVIRONMENT"
        value = var.environment
      }

      env {
        name  = "APP_ENV"
        value = var.environment
      }

      env {
        name  = "APP_REGION"
        value = var.region
      }

      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }

      env {
        name  = "USE_GOOGLE_CLOUD_LOGGING"
        value = true
      }

      env {
        name  = "TARGET_URL"
        value = true
      }

      env {
        name  = "QUEUE_LOCATION"
        value = var.task_queue_region
      }

      env {
        name  = "QUEUE_NAME"
        value = var.task_queue_name
      }

      env {
        name  = "FIRESTORE_DB"
        value = var.firestore_database_name
      }

      env {
        name  = "FIRESTORE_DOCUMENT_DEDUP"
        value = "storageflow_core_dedup"
      }

      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }

      env {
        name  = "QUEUE_LOCATION"
        value = var.task_queue_region
      }

      env {
        name  = "QUEUE_NAME"
        value = var.task_queue_name
      }

      env {
        name  = "DATAPROC_REGION"
        value = var.region
      }

      env {
        name  = "GCS_TEMP_BUCKET"
        value = var.gcs_temp_bucket
      }

      env {
        name  = "LOGGING_LEVEL"
        value = var.logging_level
      }

      env {
        name  = "LOGGING_ENV"
        value = "GCP" #see src/storageflow/core/logging.py
      }

      env {
        name  = "WORKFLOW_NAME"
        value = var.workflow_name
      }

      env {
        name  = "WORKFLOW_LOCATION"
        value = var.workflow_location
      }

      env {
        name  = "USER_CONFIG_PATH"
        value = "/app/config/user"
      }

      env {
        name  = "ROOT_CONFIG_PATH"
        value = "/app/config/root"
      }

      env {
        name  = "SERVICE_ACCOUNT"
        value = var.service_account
      }

      env {
        name  = "HEADERS_RUN_UUID_NAME"
        value = "x-storageflow-run-uuid"
      }

      env {
        name  = "WORKFLOW_MAX_RETRIES"
        value = 5
      }

      env {
        name  = "WORKFLOW_POLL_INTERVAL_SECONDS"
        value = 120
      }

    }
    scaling {
      max_instance_count = 30
      min_instance_count = 1
    }

    vpc_access {
      network_interfaces {
        network    = var.network_name
        subnetwork = var.subnetwork_name
        tags       = var.gce_tags
      }
      egress = "ALL_TRAFFIC"
    }
    service_account = var.service_account
  }
  depends_on = [null_resource.build_image]
}


