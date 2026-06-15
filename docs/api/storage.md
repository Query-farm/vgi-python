# State Storage

Stateful functions (notably distributed aggregates) persist per-group state through a
`FunctionStorage` backend so it survives across worker invocations and processes. The default is
SQLite; Azure SQL and Cloudflare Durable Object backends are available for multi-worker
deployments.

## Function storage

::: vgi.function_storage

## Azure SQL backend

Requires `pip install vgi-python[azure]`.

::: vgi.function_storage_azure_sql

## Cloudflare Durable Object backend

::: vgi.function_storage_cf_do
