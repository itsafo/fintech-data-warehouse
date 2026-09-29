# Contributing

Every change starts with a GitHub issue and lands on `main` through a pull request.

## Workflow
1. Pick or create an issue.
2. Branch from `main`: `<type>/<issue-number>-<short-slug>`
3. Commit using the convention below.
4. Open a PR whose body contains `Closes #<issue>` so the issue closes on merge.
5. Wait for CI, then squash-merge.

## Types
| Type | Use for |
|---|---|
| `feat` | New pipeline, model, or capability |
| `fix` | Bug fix |
| `test` | Adding or changing tests |
| `ci` | GitHub Actions workflows |
| `infra` | Terraform / Docker / deployment |
| `docs` | README, this guide, comments |
| `chore` | Housekeeping, dependencies |

## Branch names
`<type>/<issue-number>-<short-slug>`, lowercase, hyphens, e.g. `test/4-extractor-unit-tests`.

## Commits and PR titles
[Conventional Commits](https://www.conventionalcommits.org/) with the issue number:

`type(scope): imperative subject (#issue)`

Scopes: `terraform`, `airflow`, `dbt`, `sql`, `ci`, `docs`.
Examples: `fix(dbt): dedupe stg_fx_rates on latest capture (#3)`, `test(airflow): add binance extractor tests (#4)`.

PRs are squash-merged, so the PR title becomes the commit message on `main`.

## Never commit
`.env`, `*.tfstate`, credentials or tokens. Use `.env.example` and GitHub secrets.
