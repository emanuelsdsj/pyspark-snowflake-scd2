from datetime import date, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

TRACKED_COLUMNS = ["category", "price", "supplier"]
BUSINESS_KEY = "product_id"


def with_row_hash(df: DataFrame, tracked_cols: list[str] = TRACKED_COLUMNS) -> DataFrame:
    return df.withColumn(
        "row_hash",
        F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in tracked_cols]), 256),
    )


def build_scd2_changes(
    existing_df: DataFrame,
    incoming_df: DataFrame,
    run_date: date,
    business_key: str = BUSINESS_KEY,
    tracked_cols: list[str] = TRACKED_COLUMNS,
) -> tuple[DataFrame, DataFrame]:
    """Compares the current dimension state against an incoming snapshot.

    Returns (to_expire, to_insert):
      to_expire: existing rows whose business key changed, with end_date/is_current set
      to_insert: brand new rows and new versions of changed rows
    """
    current = with_row_hash(existing_df.filter(F.col("is_current")), tracked_cols)
    incoming = with_row_hash(incoming_df, tracked_cols)

    joined = incoming.alias("inc").join(
        current.alias("cur"), on=business_key, how="left"
    )

    new_rows = joined.filter(F.col("cur.row_hash").isNull())
    changed_rows = joined.filter(
        F.col("cur.row_hash").isNotNull() & (F.col("cur.row_hash") != F.col("inc.row_hash"))
    )

    select_cols = [business_key, "product_name", "category", "price", "supplier", "row_hash"]

    to_insert = (
        new_rows.select(*[F.col(f"inc.{c}") for c in select_cols])
        .withColumn("effective_date", F.lit(run_date))
        .withColumn("end_date", F.lit(None).cast("date"))
        .unionByName(
            changed_rows.select(*[F.col(f"inc.{c}") for c in select_cols])
            .withColumn("effective_date", F.lit(run_date))
            .withColumn("end_date", F.lit(None).cast("date"))
        )
    )

    expire_date = run_date - timedelta(days=1)
    to_expire = changed_rows.select(F.col(f"cur.{business_key}").alias(business_key)).withColumn(
        "end_date", F.lit(expire_date)
    )

    return to_expire, to_insert
