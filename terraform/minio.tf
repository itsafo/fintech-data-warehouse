# S3-compatible object storage for the true bronze archive: extractors
# write the untouched raw API response here (see
# orchestration/include/storage/object_store.py) before parsing it into
# the append-only Postgres bronze.raw_* tables.

locals {
  # S3/MinIO bucket names can't contain underscores, unlike the Postgres
  # container/volume names elsewhere in this module.
  bucket_workspace_suffix = local.is_prod ? "-prod" : ""
  minio_container_name    = "minio${local.workspace_suffix}"
}

resource "docker_image" "minio" {
  name         = var.minio_image
  keep_locally = true
}

resource "docker_volume" "minio_data" {
  name = "minio_data${local.workspace_suffix}"
}

resource "docker_container" "minio" {
  name    = local.minio_container_name
  image   = docker_image.minio.image_id
  command = ["server", "/data", "--console-address", ":9001"]

  env = [
    "MINIO_ROOT_USER=${var.minio_root_user}",
    "MINIO_ROOT_PASSWORD=${var.minio_root_password}",
  ]

  networks_advanced {
    name = docker_network.data_platform_net.name
  }

  ports {
    internal = 9000 # S3 API
    external = var.minio_api_port
  }

  ports {
    internal = 9001 # web console
    external = var.minio_console_port
  }

  volumes {
    volume_name    = docker_volume.minio_data.name
    container_path = "/data"
  }

  restart = "unless-stopped"
}

# The MinIO image ships without curl/mc, so unlike postgres.tf there's
# no docker-level healthcheck to poll here -- this fixed wait is what the
# minio_* resources below (and the pipeline's own MinIO client) rely on for
# "the API is actually up" instead.
resource "time_sleep" "wait_for_minio" {
  depends_on      = [docker_container.minio]
  create_duration = "10s"
}

resource "minio_s3_bucket" "bronze_raw" {
  bucket        = "${var.minio_bucket_name}${local.bucket_workspace_suffix}"
  acl           = "private"
  force_destroy = false
  depends_on    = [time_sleep.wait_for_minio]
}

# Least-privilege service identity for the pipeline -- scoped to this one
# bucket, never MinIO's root credentials. Mirrors pipeline_writer in
# database.tf for Postgres.
resource "minio_iam_user" "pipeline_writer" {
  name          = "pipeline-writer${local.bucket_workspace_suffix}"
  force_destroy = true
  depends_on    = [time_sleep.wait_for_minio]
}

resource "minio_iam_policy" "bronze_raw_rw" {
  name = "bronze-raw-rw${local.bucket_workspace_suffix}"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "BronzeRawReadWrite"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${minio_s3_bucket.bronze_raw.bucket}",
          "arn:aws:s3:::${minio_s3_bucket.bronze_raw.bucket}/*",
        ]
      }
    ]
  })
  depends_on = [time_sleep.wait_for_minio]
}

resource "minio_iam_user_policy_attachment" "pipeline_writer_attach" {
  user_name   = minio_iam_user.pipeline_writer.id
  policy_name = minio_iam_policy.bronze_raw_rw.id
}

# Generates the actual access_key/secret_key pair the DAG's minio_bronze
# Airflow Connection uses (see README "Bootstrap" step 1 for how to read
# these out via `terraform output`).
resource "minio_iam_service_account" "pipeline_writer_keys" {
  target_user = minio_iam_user.pipeline_writer.name
  depends_on  = [minio_iam_user_policy_attachment.pipeline_writer_attach]
}
