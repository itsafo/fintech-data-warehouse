resource "docker_image" "postgres" {
  name         = var.postgres_image
  keep_locally = true
}

# Named volume is a resource independent of the container's lifecycle, so a
# targeted `terraform destroy -target=docker_container.local_warehouse`
# tears down compute while this (and the data in it) survives. A full
# `terraform destroy` removes it too, by design -- see README "Teardown".
resource "docker_volume" "warehouse_data" {
  name = "local_warehouse_data${local.workspace_suffix}"
}

resource "docker_container" "local_warehouse" {
  name  = local.container_name
  image = docker_image.postgres.image_id

  env = [
    "POSTGRES_USER=${var.postgres_user}",
    "POSTGRES_PASSWORD=${var.postgres_password}",
    "POSTGRES_DB=${var.postgres_db}",
  ]

  networks_advanced {
    name = docker_network.data_platform_net.name
  }

  ports {
    internal = 5432
    external = var.postgres_port
  }

  volumes {
    volume_name    = docker_volume.warehouse_data.name
    container_path = "/var/lib/postgresql/data"
  }

  healthcheck {
    test     = ["CMD-SHELL", "pg_isready -U ${var.postgres_user} -d ${var.postgres_db}"]
    interval = "5s"
    timeout  = "5s"
    retries  = 10
  }

  restart = "unless-stopped"
}

# Postgres reports "started" before it's actually ready to accept
# connections; the postgresql provider resources below depend on this so
# schema/role creation doesn't race the container's init scripts.
resource "time_sleep" "wait_for_postgres" {
  depends_on      = [docker_container.local_warehouse]
  create_duration = "10s"
}
