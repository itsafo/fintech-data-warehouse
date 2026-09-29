# Schemas, roles, and grants are declared here as real Terraform resources
# (via the postgresql provider) rather than left to a manual SQL script --
# this is the "IaC-managed RBAC" piece of the platform.

locals {
  medallion_schemas = ["control", "bronze", "silver", "gold"]
}

resource "postgresql_schema" "schemas" {
  for_each   = toset(local.medallion_schemas)
  name       = each.value
  depends_on = [time_sleep.wait_for_postgres]
}

# Write access for the ingestion + dbt layers (control/bronze/silver).
# dev_user is granted membership below so the pipeline can run under this
# role's privileges without introducing a second set of connection creds.
resource "postgresql_role" "pipeline_writer" {
  name       = "pipeline_writer"
  login      = true
  password   = var.pipeline_writer_password
  depends_on = [time_sleep.wait_for_postgres]
}

# Read-only role scoped to gold, intended for BI/analyst tool connections.
# Not used by the pipeline itself -- provisioned here to prove the
# least-privilege boundary exists as code, ready to hand to a real client.
resource "postgresql_role" "analyst_reader" {
  name       = "analyst_reader"
  login      = true
  password   = var.analyst_reader_password
  depends_on = [time_sleep.wait_for_postgres]
}

resource "postgresql_grant_role" "dev_user_is_pipeline_writer" {
  role       = var.postgres_user
  grant_role = postgresql_role.pipeline_writer.name
  depends_on = [postgresql_role.pipeline_writer]
}

resource "postgresql_grant" "pipeline_writer_rw" {
  for_each    = toset(["control", "bronze", "silver"])
  database    = var.postgres_db
  role        = postgresql_role.pipeline_writer.name
  schema      = each.value
  object_type = "table"
  privileges  = ["SELECT", "INSERT", "UPDATE", "DELETE"]
  depends_on  = [postgresql_schema.schemas]
}

# Covers tables dbt/extractors create *after* this apply (existing objects
# only get privileges from postgresql_grant above).
resource "postgresql_default_privileges" "pipeline_writer_future_rw" {
  for_each    = toset(["control", "bronze", "silver"])
  database    = var.postgres_db
  role        = postgresql_role.pipeline_writer.name
  owner       = var.postgres_user
  schema      = each.value
  object_type = "table"
  privileges  = ["SELECT", "INSERT", "UPDATE", "DELETE"]
  depends_on  = [postgresql_schema.schemas]
}

resource "postgresql_grant" "analyst_reader_ro" {
  database    = var.postgres_db
  role        = postgresql_role.analyst_reader.name
  schema      = "gold"
  object_type = "table"
  privileges  = ["SELECT"]
  depends_on  = [postgresql_schema.schemas]
}

resource "postgresql_default_privileges" "analyst_reader_future_ro" {
  database    = var.postgres_db
  role        = postgresql_role.analyst_reader.name
  owner       = var.postgres_user
  schema      = "gold"
  object_type = "table"
  privileges  = ["SELECT"]
  depends_on  = [postgresql_schema.schemas]
}
