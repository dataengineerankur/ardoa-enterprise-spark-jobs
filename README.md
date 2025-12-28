# Repo B — Enterprise Spark / Data Processing Jobs

This repository represents a realistic **enterprise Spark jobs codebase** that is **triggered by Airflow** and depends on a shared library repo (Repo C).

## What this repo does
- Contains PySpark-style batch jobs (can be run in “local simulation mode” for testing without a real Spark cluster).
- Reads/writes data under a consistent `data/` layout (raw → staged → curated).
- Implements job contracts (schemas) using Repo C (`enterprise_shared`).

## Downstream / upstream dependencies
- **Triggered by**: Repo A (Airflow orchestration) via “Spark submit” (or a REST submit wrapper).
- **Depends on**: Repo C (Shared Libraries) for:
  - Schema definitions and validation
  - Contract checks and compatibility utilities

## Folder structure
- `src/enterprise_spark/`
  - `jobs/` — job entrypoints (what Airflow calls)
  - `io/` — read/write helpers, atomic write utilities
  - `quality/` — validation + DQ checks (calls into Repo C)
  - `config/` — configuration and feature flags
- `tests/`
  - `unit/` — fast tests
  - `spark/` — “spark-ish” tests (runs in local simulation mode)
- `data/` — local test data and output (for demo only)

## Intentional failure scenarios (for ARDOA black-box testing)
These are embedded to simulate real-world failures without teaching the agent anything special.

- **Schema drift**: job reads a “v2” payload while downstream expects “v1”.
- **Partial writes**: job writes half an output file and fails mid-way (non-atomic write bug).
- **Silent corruption**: job incorrectly rounds currency values (logic bug).
- **Flaky external API**: job simulates intermittent 429/503 behavior during enrichment.

How to trigger these failures is documented in Repo A’s test plan section, since Airflow is the entry point.


