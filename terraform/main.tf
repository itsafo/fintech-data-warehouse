terraform {
  required_version = ">= 1.5.0"

  # State lives in Terraform Cloud's free tier (locking + shared state,
  # not just a laptop-local .tfstate file). `terraform workspace select
  # <name>` still works exactly as before -- the prefix maps it onto a TFC
  # workspace named "fintech-data-warehouse-<name>" (e.g. fintech-data-warehouse-default,
  # fintech-data-warehouse-prod), auto-created on first `terraform login` + `init`.
  #
  # IMPORTANT (read this or applies will silently break): new TFC
  # workspaces default to "Remote" execution mode, which runs
  # plan/apply on HashiCorp's own infrastructure -- which cannot reach
  # your local/VM Docker daemon or "localhost" Postgres/MinIO. After the
  # first `terraform init` auto-creates each workspace, go to that
  # workspace's Settings -> General -> Execution Mode in the Terraform
  # Cloud UI and change it to "Local". See README "Production deployment"
  # for the full one-time setup.
  cloud {
    organization = "REPLACE_WITH_YOUR_TFC_ORG"
    workspaces {
      prefix = "fintech-data-warehouse-"
    }
  }

  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
    postgresql = {
      source  = "cyrilgdn/postgresql"
      version = "~> 1.21"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.11"
    }
    minio = {
      source  = "aminueza/minio"
      version = "~> 3.2"
    }
  }
}

provider "docker" {}

# Same rationale as the postgresql provider below: minio_* resources
# depend_on time_sleep.wait_for_minio (postgres.tf/minio.tf) rather than
# the provider block itself, since provider blocks can't depend_on.
provider "minio" {
  minio_server   = "localhost:${var.minio_api_port}"
  minio_user     = var.minio_root_user
  minio_password = var.minio_root_password
  minio_ssl      = false
}

# The postgresql provider connects over the port docker_container.local_warehouse
# publishes to the host. It can't declare a resource-level depends_on (provider
# blocks can't), so every postgresql_* resource below explicitly depends_on
# time_sleep.wait_for_postgres to guarantee the container is accepting
# connections before Terraform tries to manage schemas/roles inside it.
provider "postgresql" {
  host             = "localhost"
  port             = var.postgres_port
  username         = var.postgres_user
  password         = var.postgres_password
  database         = var.postgres_db
  sslmode          = "disable"
  superuser        = false
  connect_timeout  = 15
  expected_version = "15.0"
}

locals {
  # default/test workspace -> no suffix ("local_warehouse"); any other
  # workspace (e.g. "prod") -> suffixed ("local_warehouse_prod").
  workspace_suffix = terraform.workspace == "default" ? "" : "_${terraform.workspace}"
  container_name   = "local_warehouse${local.workspace_suffix}"
}
