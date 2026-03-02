# Copilot: BigQuery-Capable Planner and Schema Discovery

## Overview

Copilot uses a single production flow:

- **Runtime schema discovery** via `discover_tables` (INFORMATION_SCHEMA; marts first, then raw)
- **Synonym/concept mapping** so user terms (e.g. "revenue", "product ID") match schema columns (value, item_id, etc.)
- **Single SQL tool** `run_bigquery_sql` for SELECT-only queries across any allowed warehouse table
- **Planner** maps user intent to candidate tables and SQL templates (Pareto, top-N, revenue ranking)
- **Retry/fallback** when a query returns no rows: try next candidate (marts first, then raw), up to `COPILOT_MAX_RETRIES`

Access control is enforced by **IAM** (no hard-coded dataset whitelist in application code).

## Configuration

| Env var | Default | Description |
|--------|--------|-------------|
| `COPILOT_MAX_RETRIES` | 3 | Max replan/retry attempts when SQL returns no rows |
| `COPILOT_SCHEMA_CACHE_TTL` | 3600 | Schema discovery cache TTL in seconds |
| `COPILOT_DISCOVER_TABLES_LIMIT` | 20 | Max candidate tables returned by discover_tables |
| `MARTS_DATASET` / `MARTS_ADS_DATASET` | hypeon_marts, hypeon_marts_ads | Datasets tried first (marts); others are raw fallback |

## IAM and Security

- The service account used by the backend for BigQuery should have **BigQuery Data Viewer** (or dataset-level SELECT) on the project/datasets you want Copilot to query. No write privileges are required.
- **Terraform/IaC**: If you manage GCP with Terraform or similar, grant the Copilot service account `roles/bigquery.dataViewer` at project or dataset level. Do not grant `roles/bigquery.admin` or write roles.
- **Read-only enforcement**: All queries are validated server-side. The application rejects any statement containing: INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, TRUNCATE, GRANT, REVOKE, MERGE, EXPORT. Do not rely solely on the LLM to avoid DML/DDL.
- **Auditing**: Log executed SQL (and optionally table/schema) for audit. Redact PII in logs if required by policy.

## Tools

- **discover_tables(intent)**  
  Returns a ranked list of candidate tables (project, dataset, table, columns). Marts datasets first, then raw. Ranking uses synonym-aware keyword match (concept_map).

- **run_bigquery_sql(query, dry_run?)**  
  Executes a single SELECT (or WITH ... SELECT). Returns rows, schema, row_count, stats. Fails on non–read-only SQL.

## Flow

1. User asks a question.
2. Planner derives intent and calls discovery → ranked candidates (marts first, then raw) and SQL templates (with synonym resolution for revenue, product_id, etc.).
3. Handler runs each template via `run_bigquery_sql` in order until one returns a valid result (validator checks non-empty, basic sanity).
4. If no rows or invalid: replan (alternate tables/templates) and retry, up to `COPILOT_MAX_RETRIES`.
5. On success: LLM formats the result (markdown tables/lists) and returns answer + data; on failure: message listing queries tried.

## Files

- `backend/app/copilot/chat_handler.py` — Single flow: planner → run_bigquery_sql → validate → format
- `backend/app/copilot/tools.py` — `discover_tables`, `run_bigquery_sql`, `COPILOT_TOOLS`
- `backend/app/copilot/planner.py` — intent → candidates → SQL templates (revenue/Pareto/top-N; synonym resolution)
- `backend/app/copilot/concept_map.py` — User terms → column synonyms (revenue, product_id, channel, etc.)
- `backend/app/copilot/schema_cache.py` — Cache for discovery (Redis or in-memory)
- `backend/app/copilot/validator.py` — Result validation (non-empty, numeric sanity)
- `backend/app/copilot/defaults.py` — MAX_RETRIES, SCHEMA_CACHE_TTL, etc.
- `backend/app/clients/bigquery.py` — `list_tables_for_discovery`, `run_bigquery_sql_readonly`
