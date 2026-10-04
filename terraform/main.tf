terraform {
  required_version = ">= 1.5.0"

  # State lives in Terraform Cloud's free tier (locking + shared state,
  # not just a laptop-local .tfstate file). The `cloud` block selects
  # workspaces by tag (it has no `prefix` option -- that is the old `remote`
  # backend). Two workspaces share the tag, and their names decide the
  # environment (see locals below):
  #   fintech-data-warehouse-dev   -> unsuffixed resources (local_warehouse)
  #   fintech-data-warehouse-prod  -> "_prod" resources (local_warehouse_prod)
  # The first `terraform init` in a fresh org prompts for a workspace name
  # and creates it with the tag (type fintech-data-warehouse-dev). After
  # that, TF_WORKSPACE=<name> selects an EXISTING workspace non-interactively
  # (it fails if the workspace doesn't exist yet).
  #
  # IMPORTANT (read this or applies will silently break): new TFC
  # workspaces default to "Remote" execution mode, which runs
  # plan/apply on HashiCorp's own infrastructure -- which cannot reach
  # your local/VM Docker daemon or "localhost" Postgres/MinIO. After the
  # first `terraform init` creates each workspace, go to that
  # workspace's Settings -> General -> Execution Mode in the Terraform
  # Cloud UI and change it to "Local". See README "Production deployment"
  # for the full one-time setup.
  cloud {
    organization = "AbdulAnalytics"
    workspaces {
      tags = ["fintech-data-warehouse"]
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
  # A workspace whose name ends in "-prod" gets suffixed resources
  # ("local_warehouse_prod"); anything else (the dev workspace, or a local
  # "default" workspace) is unsuffixed ("local_warehouse").
  is_prod          = endswith(terraform.workspace, "-prod") || terraform.workspace == "prod"
  workspace_suffix = local.is_prod ? "_prod" : ""
  container_name   = "local_warehouse${local.workspace_suffix}"
}
