# Custom bridge network shared by the warehouse container and, via
# orchestration/docker-compose.override.yml, the Astro/Airflow containers.
resource "docker_network" "data_platform_net" {
  name   = var.network_name
  driver = "bridge"
}
