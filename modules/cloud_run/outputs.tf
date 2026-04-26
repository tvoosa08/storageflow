output "service_url" {
  value = google_cloud_run_v2_service.api_service.urls[0]
}

output "service_uri" {
  value = google_cloud_run_v2_service.api_service.uri
}

output "service_name" {
  value = google_cloud_run_v2_service.api_service.name
}
