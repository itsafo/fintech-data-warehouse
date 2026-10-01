# fintech-data-warehouse

A zero-spend (£0) data platform — a fully containerised Modern Data Stack: Terraform-managed infra, a metadata-driven multi-API ingestion layer with parallel + near-real-time Airflow orchestration, an immutable object-storage bronze archive, and a dbt medallion (bronze/silver/gold) transformation layer. Dev is entirely local at £0.00. Production is genuinely optional — see [Production deployment](#production-deployment) — and when used, still £0.00: a free-forever cloud VM (Oracle Cloud Always Free) plus Terraform Cloud's free tier for state, not a paid cloud account.

```
control.api_sources (metadata, owning_dag) ──drives──▶ two Airflow DAGs
                                                            │
      ┌───────────────────────────────────────────────────┴───────────────────────────────────┐
      ▼ daily_pipeline (@daily)                                    ▼ crypto_realtime_pipeline (*/5 * * * *)
open_meteo extractor      frankfurter extractor                          binance extractor
      │                          │                                              │
      ▼ archive raw JSON, then append ─────────────────────────────────────────▼
                        MinIO bucket `bronze-raw`  +  Postgres bronze.raw_* (append-only)
      │                          │                                              │
      ▼                          ▼                                              ▼
bronze.raw_weather        bronze.raw_fx                                bronze.raw_crypto
      └──────────────────────────┴──────────────────────────────────────────────┘
                                  ▼  (daily_pipeline's dbt run only -- not every 5 min)
                     dbt staging (latest-of-day dedup for weather/fx, full-grain crypto)
                                  ▼
                     dbt intermediate (silver) + marts (gold)
                                  ▼
       fct_market_snapshot / dim_date / fct_crypto_price_ticks (incremental, full-resolution)
```

## What's here

| Layer | Tool | Path |
|---|---|---|
| IaC | Terraform (`kreuzwerker/docker`, `cyrilgdn/postgresql`, `aminueza/minio`) | [terraform/](terraform/) |
| Control plane / bronze DDL | SQL | [sql/control_schema.sql](sql/control_schema.sql) |
| Object storage (raw bronze archive) | MinIO (S3-compatible) | [terraform/minio.tf](terraform/minio.tf) |
| Transformation | dbt-core (`dbt-postgres`) | [dbt/](dbt/) |
| Orchestration (dev) | Apache Airflow via Astro CLI (2 DAGs) | [orchestration/](orchestration/) |
| Orchestration (prod) | Apache Airflow via standalone `docker compose` | [orchestration/docker-compose.prod.yaml](orchestration/docker-compose.prod.yaml) |
| CI | GitHub Actions | [.github/workflows/](.github/workflows/) |
| CD (manual-trigger) | GitHub Actions over SSH | [.github/workflows/deploy_prod.yml](.github/workflows/deploy_prod.yml) |

**Design notes** (the "why" behind non-obvious choices):
- **Bronze is two things, not one.** MinIO holds the untouched raw JSON response per poll (the true immutable archive); Postgres `bronze.raw_*` holds the parsed/typed rows dbt reads, **append-only** — every poll is a plain `INSERT`, never an upsert, with a `minio_object_key` column pointing back to the exact raw object. dbt itself only models silver (`models/staging`, `models/intermediate`) and gold (`models/marts`), reading bronze via `sources.yml`.
- **Two DAGs share one metadata table.** `control.api_sources.owning_dag` scopes each row to `daily_pipeline` (weather, fx — genuinely don't change faster than daily) or `crypto_realtime_pipeline` (binance, polled every 5 min). The realtime DAG only extracts+loads — no dbt run every 5 minutes, that would be wasteful; the daily DAG's dbt run picks up everything accumulated since.
- **`control.api_sources` is the single source of truth for ingestion.** Add a row + one line in `orchestration/include/extractors/registry.py` to onboard a 4th API — neither DAG's code changes, and Airflow's dynamic task mapping (`.expand()`) fans out over however many active rows match its `owning_dag`.
- **Dedup strategy differs by source, on purpose.** Weather/fx staging models collapse bronze to the latest observation per day (`row_number()` over `captured_at`); crypto's `stg_crypto_prices` stays full-grain — full resolution is the entire point of 5-minute polling. `fct_crypto_price_ticks` is the first incremental dbt model here, surfacing that history without a full rebuild every run.
- **RBAC is Terraform-managed**, not manual SQL: `pipeline_writer`/`analyst_reader` Postgres roles (write on control/bronze/silver vs. read-only on gold), and a scoped MinIO `pipeline_writer` IAM user limited to the `bronze-raw` bucket — the pipeline never uses MinIO's root credentials.
- **dev vs prod schema promotion**: dbt's `dev` target writes to `dev_schema_silver`/`dev_schema_gold`; `prod` writes to the literal `silver`/`gold` schemas Terraform created (and `analyst_reader` has grants on) — see `dbt/macros/generate_schema_name.sql`.

## Prerequisites

Install these on your machine (all free):
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Terraform CLI](https://developer.hashicorp.com/terraform/install) (>= 1.5)
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)
- `psql` (Postgres client) — `brew install libpq && brew link --force libpq` on macOS
- Python 3.11+ (only needed if you want to run dbt/extractor code outside containers)
- A free [Terraform Cloud](https://app.terraform.io/) account + `terraform login` — state now lives there instead of a local `.tfstate` file (see `terraform/main.tf`'s `cloud` block), so this is required for local dev too, not just prod. Create an organization, then substitute it for `REPLACE_WITH_YOUR_TFC_ORG` in `terraform/main.tf`.

## Bootstrap, in order

**1. Infra: network, Postgres, MinIO, schemas, RBAC, control tables**
```bash
cd terraform
cp ../.env.example ../.env   # then edit ../.env with real values
terraform init               # first time: when prompted, type the name fintech-data-warehouse-dev to create it
export TF_WORKSPACE=fintech-data-warehouse-dev   # later runs: select it non-interactively
terraform workspace show     # fintech-data-warehouse-dev = dev
terraform apply -var-file=environments/dev.tfvars
```
This creates the `data_platform_net` network; the `local_warehouse` Postgres container with the `control`/`bronze`/`silver`/`gold` schemas, `pipeline_writer`/`analyst_reader` roles, and `sql/control_schema.sql` applied (control-plane tables, append-only bronze tables, the 3 seeded API sources); and the `minio` container with the `bronze-raw` bucket and a scoped `pipeline_writer` service account.

**Grab the MinIO service account keys** (Terraform generates them, they're not knowable ahead of time):
```bash
terraform output minio_pipeline_writer_access_key
terraform output -raw minio_pipeline_writer_secret_key
```
Paste both into `orchestration/airflow_settings.yaml`'s `minio_bronze` connection (`conn_login`/`conn_password`), replacing the `REPLACE_WITH_TERRAFORM_OUTPUT_*` placeholders.

**2. Verify dbt can connect**
```bash
cd ../dbt
dbt deps --profiles-dir .
dbt debug --profiles-dir . --target dev
```

**3. Start Airflow (Astro)**
```bash
cd ../orchestration
astro dev start
```
`docker-compose.override.yml` attaches the Airflow containers to `data_platform_net` (must already exist — step 1) and mounts `../dbt` into the containers at `/usr/local/airflow/dbt`.

**4. Set the SMTP env vars** (for failure/summary emails) in `.env` at the repo root before `astro dev start` — Astro loads `.env` automatically. The `local_warehouse` and `minio_bronze` Airflow Connections are pre-created via `orchestration/airflow_settings.yaml` (once you've pasted in the MinIO keys from step 1).

**5. Trigger the DAGs**
Open the Airflow UI (`http://localhost:8080`, default `admin`/`admin` in local dev) and unpause + trigger both `api_to_analytics_pipeline` (daily) and `crypto_realtime_pipeline` (every 5 min), or:
```bash
astro dev bash -c "airflow dags trigger api_to_analytics_pipeline"
astro dev bash -c "airflow dags unpause crypto_realtime_pipeline"   # it'll then run on its own schedule
```
Watch `bronze.raw_*` populate (and the raw JSON land in MinIO — console at `http://localhost:9001`), then `silver`/`gold` after `api_to_analytics_pipeline`'s dbt tasks run. Query `control.pipeline_run_log` and `control.error_log` for per-source observability.

## Teardown / volume retention

`docker_volume.warehouse_data` and `docker_volume.minio_data` are resources independent of their containers, so:
```bash
terraform destroy -target=docker_container.local_warehouse -target=docker_container.minio   # drops compute, keeps data
terraform destroy                                                                             # drops everything, including both volumes
```
Re-running `terraform apply` after a container-only destroy reattaches to the existing volumes — your data survives the loop. A full `destroy` is a deliberate, one-way reset.

## Testing the "prod" Terraform workspace locally

This spins up a *second* local instance under the `prod` workspace/schema — useful for testing prod-shaped config before it ever touches the real VM, but **it is not the production deployment** (that's the next section).
```bash
terraform init                # first time: type fintech-data-warehouse-prod when prompted to create it
export TF_WORKSPACE=fintech-data-warehouse-prod
terraform apply -var-file=environments/prod.tfvars
```
The container is named `local_warehouse_prod`; point `dbt --target prod` at it if you want to test the prod dbt schema promotion locally too.

## Production deployment

A genuinely always-on deployment, independent of your laptop: Oracle Cloud's **Always Free** tier (not a 12-month trial — free forever) for the VM, Terraform Cloud's free tier for shared/locked state, and a manual-trigger GitHub Actions workflow for deploys. Nothing here is exposed to the public internet except SSH — see [orchestration/docker-compose.prod.yaml](orchestration/docker-compose.prod.yaml) for why this can't just be `astro dev start` on a server (Astro CLI is a local dev tool only).

### One-time VM setup

1. Create an Oracle Cloud account (free) and launch an **Always Free Ampere A1** instance — Ubuntu 22.04, 2–4 OCPU / 12–24GB RAM (enough for Postgres + MinIO + Airflow together; a GCP e2-micro or AWS free-tier t2.micro would be too small for this whole stack). If you hit an "out of capacity" error, that's a known Ampere A1 quirk in busy regions — retry, try a different availability domain, or try again later; it isn't something on your end.
2. In the VM's **Security List** (OCI's cloud-level firewall, separate from the OS firewall — both must allow it), open **only port 22**. Nothing else — Postgres/MinIO/Airflow all stay off the public internet by design.
3. SSH in and install: Docker + the Compose plugin ([docs](https://docs.docker.com/engine/install/ubuntu/)), [Terraform CLI](https://developer.hashicorp.com/terraform/install), `git`. Create a non-root deploy user in the `docker` group; add its SSH public key to the VM, keep the private key for the GitHub secret below.
4. `git clone` this repo onto the VM (path becomes the `DEPLOY_REPO_PATH` secret below).

### One-time Terraform Cloud setup

1. Free account at [app.terraform.io](https://app.terraform.io/), create an organization, generate an API token.
2. Substitute your org into `terraform/main.tf`'s `cloud { organization = ... }` block.
3. **After the first `terraform init`/`apply` auto-creates each workspace** (`fintech-data-warehouse-dev`, `fintech-data-warehouse-prod`), go to that workspace's *Settings → General → Execution Mode* in the TFC UI and change it to **Local**. New TFC workspaces default to *Remote* execution, which runs `apply` on HashiCorp's own infrastructure — which can't reach your Docker daemon. Skipping this step is the single most likely way this silently breaks.

### First deploy (manual, once — confirms everything works before automating)

On the VM: write a real `.env` (see `.env.example`'s "Production deploy" section), then run the same steps `deploy_prod.yml` automates —
```bash
cd terraform && terraform init && export TF_WORKSPACE=fintech-data-warehouse-prod && terraform apply -var-file=environments/prod.tfvars
terraform output minio_pipeline_writer_access_key            # capture these two --
terraform output -raw minio_pipeline_writer_secret_key       # -- you need them for AIRFLOW_CONN_MINIO_BRONZE and the GitHub secrets below
cd .. && docker compose -f orchestration/docker-compose.prod.yaml up -d --build
```

### GitHub Actions secrets (`.github/workflows/deploy_prod.yml`)

| Secret | Purpose |
|---|---|
| `DEPLOY_SSH_HOST`, `DEPLOY_SSH_USER`, `DEPLOY_SSH_PRIVATE_KEY`, `DEPLOY_SSH_KNOWN_HOSTS`, `DEPLOY_REPO_PATH` | SSH connection to the VM |
| `TF_API_TOKEN` | Terraform Cloud API token, used non-interactively via `TF_TOKEN_app_terraform_io` |
| `PROD_POSTGRES_PASSWORD`, `PROD_PIPELINE_WRITER_PASSWORD`, `PROD_ANALYST_READER_PASSWORD` | Postgres + RBAC, same roles as dev |
| `PROD_MINIO_ROOT_PASSWORD` | MinIO root (Terraform-only; the pipeline never uses it) |
| `PROD_MINIO_PIPELINE_WRITER_ACCESS_KEY`, `PROD_MINIO_PIPELINE_WRITER_SECRET_KEY` | From the **first manual deploy's** `terraform output` — chicken-and-egg, can't exist before that |
| `PROD_AIRFLOW_FERNET_KEY`, `PROD_AIRFLOW_WEBSERVER_SECRET_KEY` | Generate once per `.env.example`'s comments, keep stable |
| `PROD_AIRFLOW_METADATA_DB_PASSWORD`, `PROD_AIRFLOW_ADMIN_PASSWORD` | Prod Airflow's own metadata DB + UI login |
| `PROD_SMTP_HOST`, `PROD_SMTP_USER`, `PROD_SMTP_PASSWORD`, `PROD_ALERT_EMAIL_TO` | Failure/summary emails |

After the first manual deploy above, add all of these, then use the **Deploy to Production** button under the Actions tab for every subsequent deploy.

### Viewing the Airflow UI

Nothing's public, so tunnel in:
```bash
ssh -L 8080:localhost:8080 <deploy-user>@<vm-ip>
```
then open `http://localhost:8080`.

## CI/CD secrets (GitHub Actions, local-workflow validation)

Add these as **repository secrets** (Settings → Secrets and variables → Actions), never as plain workflow env vars:
- `TF_POSTGRES_PASSWORD`, `TF_PIPELINE_WRITER_PASSWORD`, `TF_ANALYST_READER_PASSWORD`, `TF_MINIO_ROOT_PASSWORD` — used by `.github/workflows/terraform_ci.yml`'s best-effort `terraform plan` (a PR-validation gate, distinct from the `PROD_*` deploy secrets above).

`.github/workflows/dbt_ci.yml` and `airflow_dag_tests.yml` don't need secrets — they spin up an ephemeral `postgres:15-alpine` service container / a plain `DagBag` load, so credentials never leave the runner.

## Phase 2 (not built yet)

Deliberately out of scope for this first pass — documented here so the gap is visible, not silent:
- **HashiCorp Vault** (local container) for secrets instead of Airflow Connections/`.env` — the more "real" enterprise secrets story.
- **Grafana** (local container) reading `control.pipeline_run_log`/`error_log` for a pipeline-health dashboard (rows loaded, duration, failure rate over time) instead of querying those tables by hand.

Both are free/Docker-based and would slot in without changing the ingestion or transformation layers — ask if you want either built out.

## Troubleshooting

- `terraform apply` fails on the `postgresql_*`/`minio_*` resources: containers need ~10s to accept connections after they report started (`time_sleep.wait_for_postgres`/`wait_for_minio` handle this) — if it still fails, `docker ps` to confirm both are healthy, then `terraform apply` again (idempotent).
- `dbt debug` can't connect: confirm `POSTGRES_PORT` in your shell matches the workspace you applied (`5432` dev / `5433` prod).
- DAG's `dbt_*` tasks fail with "command not found": `dbt-postgres` installs into the Astro image via `orchestration/requirements.txt` — rebuild with `astro dev restart` after any requirements change.
- `extract_and_load` tasks fail with a MinIO/boto3 connection or auth error: confirm `orchestration/airflow_settings.yaml`'s `minio_bronze` connection has the *real* access/secret key from `terraform output`, not the `REPLACE_WITH_TERRAFORM_OUTPUT_*` placeholders — `astro dev restart` after editing.
- `crypto_realtime_pipeline` looks like it's not running: it's paused by default like any new Airflow DAG — unpause it in the UI or via `airflow dags unpause crypto_realtime_pipeline`.
- No emails arriving: check `AIRFLOW__SMTP__*` and `ALERT_EMAIL_TO` are in `.env` *before* `astro dev start` (Astro only loads `.env` at container start), and that a Gmail app password (not your account password) is used if using Gmail SMTP.
- `terraform apply` hangs or fails oddly against a Terraform Cloud workspace: almost always the execution-mode gotcha in "Production deployment → One-time Terraform Cloud setup" step 3 — the workspace defaulted to *Remote* execution and is trying (and failing) to reach your Docker daemon from HashiCorp's infrastructure instead of yours.
- `deploy_prod.yml` fails on the `docker compose up` step: SSH into the VM and check `docker compose -f orchestration/docker-compose.prod.yaml logs airflow-init` first — most first-deploy failures are a missing/blank value in the `.env` GitHub secrets block (§ Production deployment).
