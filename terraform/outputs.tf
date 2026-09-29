output "workspace" {
  value = terraform.workspace
}

output "network_name" {
  value = docker_network.data_platform_net.name
}

output "container_name" {
  value = docker_container.local_warehouse.name
}

output "postgres_host" {
  value = "localhost"
}

output "postgres_port" {
  value = var.postgres_port
}

output "postgres_db" {
  value = var.postgres_db
}

output "schemas" {
  value = [for s in postgresql_schema.schemas : s.name]
}

output "minio_endpoint" {
  value = "localhost:${var.minio_api_port}"
}

output "minio_console_url" {
  value = "http://localhost:${var.minio_console_port}"
}

output "minio_bucket_name" {
  value = minio_s3_bucket.bronze_raw.bucket
}

output "minio_pipeline_writer_access_key" {
  value = minio_iam_service_account.pipeline_writer_keys.access_key
}

output "minio_pipeline_writer_secret_key" {
  value     = minio_iam_service_account.pipeline_writer_keys.secret_key
  sensitive = true
}
