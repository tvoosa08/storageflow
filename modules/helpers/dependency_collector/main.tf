
data "external" "collect_dependencies" {
  program = ["python3", "${path.module}/collect_dependencies.py"]
  query = {
    dependencies = jsonencode(var.dependencies)
  }
}


#resource "null_resource" "test" {
#  depends_on = [data.external.collect_dependencies]
#  provisioner "local-exec" {
#    command = <<-EOT
#      echo "Dependencies collected successfully"
#      echo "Staging directory: ${data.external.collect_dependencies.result.staging_dir}"
#      echo "Combined SHA256: ${data.external.collect_dependencies.result.combined_sha256}"
#      ls -altr ${data.external.collect_dependencies.result.staging_dir}
#      tree ${data.external.collect_dependencies.result.staging_dir}
#    EOT
#  }
#
#  triggers = {
#    source_hash = data.external.collect_dependencies.result.combined_sha256
#  }
#}



# do some required output
output "staging_dir" {
  value = data.external.collect_dependencies.result.staging_dir
}

output "source_hash" {
  value = data.external.collect_dependencies.result.combined_sha256
}
