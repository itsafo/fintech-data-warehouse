# Applies sql/control_schema.sql (control-plane tables + bronze landing
# tables + seed rows for the 3 demo API sources) once schemas/grants exist.
# Requires the `psql` client on PATH -- see README prerequisites.
resource "null_resource" "bootstrap_control_schema" {
  depends_on = [
    postgresql_schema.schemas,
    postgresql_grant.pipeline_writer_rw,
    postgresql_default_privileges.pipeline_writer_future_rw,
  ]

  triggers = {
    sql_hash = filesha256("${path.module}/../sql/control_schema.sql")
  }

  provisioner "local-exec" {
    command = "psql -h localhost -p ${var.postgres_port} -U ${var.postgres_user} -d ${var.postgres_db} -v ON_ERROR_STOP=1 -f ${path.module}/../sql/control_schema.sql"
    environment = {
      PGPASSWORD = var.postgres_password
    }
  }
}
