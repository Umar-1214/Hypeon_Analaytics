# Copilot V2 Migration: BigQuery-Capable Planner and Schema Discovery

## Overview

Copilot V2 removes the marts-first, schema-locked behavior in favor of:

- **Runtime schema discovery** via `discover_tables` (INFORMATION_SCHEMA + optional cache)
- **Single SQL tool** `run_bigquery_sql` for SELECT-only queries across any allowed warehouse table (marts or raw)
- **Planner step** that maps user intent to candidate tables and SQL templates
- **Retry/fallback** when a query returns no rows (replan and try alternate tables, up to `COPILOT_MAX_RETRIES`)

Access control is enforced by **IAM** (no hard-coded dataset whitelist in application code).

## Enabling V2

Set the feature flag:

```bash
export COPILOT_V2=true
```

When `COPILOT_V2` is not set or false, the existing V1 flow (run_sql / run_sql_raw) is used.

## Configuration

| Env var | Default | Description |
|--------|--------|-------------|
| `COPILOT_V2` | (unset) | Set to `true` / `1` / `yes` to enable V2 |
| `COPILOT_MAX_RETRIES` | 3 | Max replan/retry attempts when SQL returns no rows |
| `COPILOT_SCHEMA_CACHE_TTL` | 3600 | Schema discovery cache TTL in seconds |
| `COPILOT_DISCOVER_TABLES_LIMIT` | 20 | Max candidate tables returned by discover_tables |

## IAM and Security

- The service account used by the backend for BigQuery should have **BigQuery Data Viewer** (or dataset-level SELECT) on the project/datasets you want Copilot to query. No write privileges are required.
- **Read-only enforcement**: All queries are validated server-side. The application rejects any statement containing: INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, TRUNCATE, GRANT, REVOKE, MERGE, EXPORT. Do not rely solely on the LLM to avoid DML/DDL.
- **Auditing**: Log executed SQL (and optionally table/schema) for audit. Redact PII in logs if required by policy.

## Tools (V2)

- **discover_tables(intent)**  
  Returns a ranked list of candidate tables (project, dataset, table, columns, last_updated) for the given intent. Uses schema cache when available.

- **run_bigquery_sql(query, dry_run?)**  
  Executes a single SELECT (or WITH ... SELECT). Returns rows, schema, row_count, stats. Fails on non–read-only SQL.

## Flow

1. User asks a question.
2. Planner derives intent and calls discovery → ranked candidates and SQL templates.
3. Handler runs each template (via `run_bigquery_sql`) in order until one returns a valid result (validator checks non-empty, basic sanity).
4. If no rows or invalid: replan (alternate tables/templates) and retry, up to `COPILOT_MAX_RETRIES`.
5. On success: LLM summarizes the result and returns answer + data; on failure: message listing tables tried.

## Rollout

1. Deploy with V2 behind `COPILOT_V2=true`.
2. Run unit and integration tests (including fallback behavior).
3. Enable the flag for internal users and confirm fallback and discovery in logs.
4. Flip default (e.g. `COPILOT_V2=true` by default) when stable.

## Files Changed / Added

- `backend/app/copilot/chat_handler.py` — V2 branch, planner-driven execution, retry loop
- `backend/app/copilot/tools.py` — `discover_tables`, `run_bigquery_sql`, `COPILOT_TOOLS_V2`
- `backend/app/copilot/planner.py` — intent → candidates → SQL templates
- `backend/app/copilot/schema_cache.py` — cache for discovery (Redis or in-memory)
- `backend/app/copilot/validator.py` — result validation (non-empty, numeric sanity)
- `backend/app/copilot/defaults.py` — MAX_RETRIES, SCHEMA_CACHE_TTL, etc.
- `backend/app/clients/bigquery.py` — `list_tables_for_discovery`, `run_bigquery_sql_readonly`
