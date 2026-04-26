locals {
  url        = var.service_url
  parsed_url = provider::netparse::parse_url(local.url)
}



resource "google_cloud_tasks_queue" "queue" {
  name     = var.task_queue_name
  location = var.task_queue_region
  project  = var.project_id

  http_target {
    http_method = "POST"
    uri_override {
      host   = local.parsed_url.host
      scheme = upper(local.parsed_url.scheme)
      path_override {
        path = "/tasks"
      }
    }
    oidc_token {
      service_account_email = var.service_account
      audience              = var.service_audience
    }
  }
  rate_limits {
    max_dispatches_per_second = 10
    max_concurrent_dispatches = 20
  }
  retry_config {
    max_attempts       = -1   # -1 for infinite
    max_retry_duration = "0s" # example "300s" or 0 for unlimited
    min_backoff        = "10s"
  }
}


