# Used with the "prod" workspace: terraform workspace select prod && terraform apply -var-file=environments/prod.tfvars
#
# In real use this apply runs ON the production VM itself (see README
# "Production deployment"), not your laptop, so the port-5433 split below
# isn't needed to avoid a clash with dev anymore -- dev and prod are on
# different machines entirely. Kept anyway: it's still harmless, and lets
# you spin up a local "prod" workspace for testing without colliding with
# a running dev instance.
postgres_db   = "warehouse"
postgres_port = 5433
