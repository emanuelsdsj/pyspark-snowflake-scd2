import argparse
from datetime import date, datetime

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from scd2_pipeline.config import load_snowflake_config
from scd2_pipeline.snowflake_io import read_table, run_sql, write_table
from scd2_pipeline.spark_session import build_spark_session
from scd2_pipeline.transform import build_scd2_changes, with_row_hash

DIM_TABLE = "DIM_PRODUCT_SCD2"
STAGING_TABLE = "SCD2_STAGING"

CSV_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), nullable=False),
        StructField("product_name", StringType(), nullable=True),
        StructField("category", StringType(), nullable=True),
        StructField("price", DoubleType(), nullable=True),
        StructField("supplier", StringType(), nullable=True),
    ]
)

MERGE_EXPIRE_SQL = """
MERGE INTO {dim} AS tgt
USING (SELECT product_id, end_date FROM {staging} WHERE change_type = 'EXPIRE') AS src
ON tgt.product_id = src.product_id AND tgt.is_current = TRUE
WHEN MATCHED THEN UPDATE SET tgt.end_date = src.end_date, tgt.is_current = FALSE
"""

INSERT_NEW_VERSIONS_SQL = """
INSERT INTO {dim} (
    product_id, product_name, category, price, supplier,
    row_hash, effective_date, end_date, is_current
)
SELECT product_id, product_name, category, price, supplier, row_hash, effective_date, end_date, TRUE
FROM {staging} WHERE change_type = 'INSERT'
"""


def run_initial_load(snapshot_path: str, run_date: date) -> None:
    spark = build_spark_session()
    config = load_snowflake_config()

    incoming = spark.read.csv(snapshot_path, header=True, schema=CSV_SCHEMA)
    dim_rows = (
        with_row_hash(incoming)
        .withColumn("effective_date", F.lit(run_date))
        .withColumn("end_date", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
    )

    write_table(dim_rows, config, DIM_TABLE, mode="overwrite")
    print(f"Initial load complete: {dim_rows.count()} rows written to {DIM_TABLE}")


def run_incremental_load(snapshot_path: str, run_date: date) -> None:
    spark = build_spark_session()
    config = load_snowflake_config()

    incoming = spark.read.csv(snapshot_path, header=True, schema=CSV_SCHEMA)
    existing = read_table(spark, config, DIM_TABLE)

    to_expire, to_insert = build_scd2_changes(existing, incoming, run_date)
    to_expire = to_expire.cache()
    to_insert = to_insert.cache()
    expire_count = to_expire.count()
    insert_count = to_insert.count()

    staging_expire = to_expire.withColumn("change_type", F.lit("EXPIRE")).select(
        "change_type",
        "product_id",
        F.lit(None).cast("string").alias("product_name"),
        F.lit(None).cast("string").alias("category"),
        F.lit(None).cast("double").alias("price"),
        F.lit(None).cast("string").alias("supplier"),
        F.lit(None).cast("string").alias("row_hash"),
        F.lit(None).cast("date").alias("effective_date"),
        "end_date",
    )
    staging_insert = to_insert.withColumn("change_type", F.lit("INSERT")).select(
        "change_type",
        "product_id",
        "product_name",
        "category",
        "price",
        "supplier",
        "row_hash",
        "effective_date",
        "end_date",
    )
    staging = staging_expire.unionByName(staging_insert)

    write_table(staging, config, STAGING_TABLE, mode="overwrite")

    run_sql(config, MERGE_EXPIRE_SQL.format(dim=DIM_TABLE, staging=STAGING_TABLE))
    run_sql(config, INSERT_NEW_VERSIONS_SQL.format(dim=DIM_TABLE, staging=STAGING_TABLE))

    print(f"Incremental load complete: {expire_count} rows expired, {insert_count} rows inserted")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the product catalog SCD2 pipeline")
    parser.add_argument("--snapshot", required=True, help="Path to the CSV snapshot")
    parser.add_argument(
        "--run-date", default=None, help="Run date as YYYY-MM-DD (defaults to today)"
    )
    parser.add_argument(
        "--initial-load",
        action="store_true",
        help="Write the snapshot directly into the dimension table instead of diffing",
    )
    args = parser.parse_args()

    run_date = (
        datetime.strptime(args.run_date, "%Y-%m-%d").date() if args.run_date else date.today()
    )

    if args.initial_load:
        run_initial_load(args.snapshot, run_date)
    else:
        run_incremental_load(args.snapshot, run_date)


if __name__ == "__main__":
    main()
