variable "postgres_user" {
  type        = string
  default     = "dev_user"
  description = "Superuser created by the postgres:15-alpine image; used by dbt, the DAG, and Terraform's own postgresql provider."
}

variable "postgres_password" {
  type        = string
  default     = "dev_password"
  sensitive   = true
  description = "Local-only default. Override via TF_VAR_postgres_password (or a .auto.tfvars that is gitignored) for anything beyond throwaway local dev."
}

variable "postgres_db" {
  type        = string
  default     = "warehouse"
  description = "Database name created inside the container."
}

variable "postgres_port" {
  type        = number
  default     = 5432
  description = "Host port the container's 5432 is published on. Give dev/prod workspaces different values if you want them running concurrently."
}

variable "postgres_image" {
  type        = string
  default     = "postgres:15-alpine"
  description = "Warehouse container image."
}

variable "network_name" {
  type        = string
  default     = "data_platform_net"
  description = "Docker bridge network shared with the Astro/Airflow containers (see orchestration/docker-compose.override.yml)."
}

variable "pipeline_writer_password" {
  type        = string
  sensitive   = true
  default     = "pipeline_writer_password"
  description = "Local-only default for the pipeline_writer role (write access to control/bronze/silver). Override via TF_VAR_pipeline_writer_password."
}

variable "analyst_reader_password" {
  type        = string
  sensitive   = true
  default     = "analyst_reader_password"
  description = "Local-only default for the analyst_reader role (read-only on gold). Override via TF_VAR_analyst_reader_password."
}

variable "minio_root_user" {
  type        = string
  default     = "minio_admin"
  description = "MinIO root/admin credential. Only Terraform itself uses this directly -- the pipeline connects with the scoped pipeline_writer service account instead."
}

variable "minio_root_password" {
  type        = string
  sensitive   = true
  default     = "minio_admin_password"
  description = "Local-only default. Override via TF_VAR_minio_root_password for anything beyond throwaway local dev. Must be 8+ characters (MinIO requirement)."
}

variable "minio_api_port" {
  type        = number
  default     = 9000
  description = "Host port the container's S3 API (9000) is published on."
}

variable "minio_console_port" {
  type        = number
  default     = 9001
  description = "Host port the container's web console (9001) is published on."
}

variable "minio_bucket_name" {
  type        = string
  default     = "bronze-raw"
  description = "Bucket holding the immutable raw JSON archive, one object per extractor poll."
}
