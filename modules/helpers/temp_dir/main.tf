data "external" "temp_dir" {
  program = ["bash", "${path.module}/create_temp_dir.sh"]
}

output "temp_dir_path" {
  value = data.external.temp_dir.result["temp_dir"]
}

