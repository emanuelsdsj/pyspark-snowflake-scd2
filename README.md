# pyspark-snowflake-scd2

SCD Type 2 pipeline for a product catalog. PySpark handles the diff logic
(hashing tracked attributes, detecting new/changed rows) and Snowflake stores
the versioned dimension table, applying the final upsert via `MERGE`.

## Architecture

```
CSV snapshot ──► PySpark (diff against current dimension) ──► staging table (Snowflake)
                                                                      │
                                                                      ▼
                                                        MERGE into DIM_PRODUCT_SCD2
```

- `DIM_PRODUCT_SCD2`: the versioned dimension. Each row has `effective_date`,
  `end_date`, and `is_current`. A changed product gets its old row expired and
  a new row inserted, so full history is kept.
- `SCD2_STAGING`: transient table written by Spark each run, consumed by the
  `MERGE`/`INSERT` statements. Spark's Snowflake connector doesn't support
  `MERGE` directly, so the actual upsert runs as plain SQL via
  `snowflake-connector-python`.

## Prerequisites

- Python 3.10+ (the `pyspark` version is pinned to the 3.5.x line: the
  Snowflake Spark connector is built for Scala 2.12, which PySpark 4.x no
  longer ships)
- Java 17 (required by PySpark): `java -version`
- A Snowflake account. If you don't have one, sign up for the free trial at
  https://signup.snowflake.com (30 days / $400 in credits). This is the only
  step that requires a browser; everything else, including creating the
  warehouse/database/tables, runs from the CLI.
- If your account requires MFA, password auth won't work for the CLI. Generate
  a Programmatic Access Token instead (Snowsight > Governance & security >
  Users & roles > your user > Programmatic access tokens) and use it as
  `SNOWFLAKE_PASSWORD`. PATs also require an active network policy on the
  account/user: either allow the bypass option when generating the token, or
  create a network policy first.

## Setup

```bash
pip install -e ".[dev]"
cp .env.example .env
# fill in .env with your Snowflake account identifier, user, password, etc.
```

Your Snowflake account identifier is the part before `.snowflakecomputing.com`
in your Snowsight URL (e.g. `xy12345.us-east-1`).

Create the warehouse, database, schema and tables from the CLI:

```bash
python scripts/run_sql_file.py sql/create_tables.sql
```

## Running

Generate the two synthetic snapshots:

```bash
python scripts/generate_snapshots.py
```

Run the local tests (no Snowflake needed):

```bash
pytest
```

First run: loads the initial catalog straight into the dimension table.

```bash
python -m scd2_pipeline.pipeline --snapshot data/raw/products_day1.csv --initial-load
```

Second run: diffs `products_day2.csv` against the current dimension state
and applies changes via `MERGE`.

```bash
python -m scd2_pipeline.pipeline --snapshot data/raw/products_day2.csv --run-date 2026-09-15
```

Check the result in Snowsight:

```sql
SELECT * FROM DIM_PRODUCT_SCD2 ORDER BY product_id, effective_date;
```

Products that changed between day 1 and day 2 should show two rows: one with
`is_current = FALSE` and an `end_date`, and one with `is_current = TRUE` and
no `end_date`.

You'll see a `Query pushdown is not supported` warning in the logs: the
connector build targets Spark 3.4 and this project runs 3.5.x. It's harmless
(pushdown is a read optimization, not a correctness issue) but if it bothers
you, swap `SNOWFLAKE_SPARK_PACKAGE` in `spark_session.py` for a build that
matches your exact Spark version.

## Project layout

```
sql/create_tables.sql       DDL for the warehouse, tables
scripts/generate_snapshots.py  synthetic data generator
src/scd2_pipeline/
  config.py                Snowflake credentials from .env
  spark_session.py         SparkSession wired with the Snowflake connector
  snowflake_io.py          read/write via Spark + raw SQL execution
  transform.py             the SCD2 diff logic
  pipeline.py              CLI entrypoint
tests/test_transform.py    unit tests for the diff logic (local Spark only)
```
