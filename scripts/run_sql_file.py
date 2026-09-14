"""Executes a .sql file against Snowflake using the credentials in .env.

Usage: python scripts/run_sql_file.py sql/create_tables.sql
"""

import sys
from pathlib import Path

import snowflake.connector

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from scd2_pipeline.config import load_snowflake_config  # noqa: E402


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python scripts/run_sql_file.py <path-to-sql-file>")
        raise SystemExit(1)

    sql_path = Path(sys.argv[1])
    config = load_snowflake_config()

    conn = snowflake.connector.connect(**config.as_bootstrap_kwargs())
    try:
        for cursor in conn.execute_string(sql_path.read_text()):
            for row in cursor:
                print(row)
    finally:
        conn.close()

    print(f"Executed {sql_path}")


if __name__ == "__main__":
    main()
