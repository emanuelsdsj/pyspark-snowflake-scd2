from datetime import date

import pytest
from pyspark.sql import SparkSession

from scd2_pipeline.transform import build_scd2_changes


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
    yield session
    session.stop()


def test_new_product_is_inserted(spark):
    existing = spark.createDataFrame(
        [],
        "product_id string, product_name string, category string, price double, "
        "supplier string, effective_date date, end_date date, is_current boolean",
    )
    incoming = spark.createDataFrame(
        [("P0001", "Product 1", "Electronics", 100.0, "Acme Corp")],
        ["product_id", "product_name", "category", "price", "supplier"],
    )

    to_expire, to_insert = build_scd2_changes(existing, incoming, run_date=date(2026, 1, 1))

    assert to_expire.count() == 0
    inserted = to_insert.collect()
    assert len(inserted) == 1
    assert inserted[0]["product_id"] == "P0001"
    assert inserted[0]["effective_date"] == date(2026, 1, 1)
    assert inserted[0]["end_date"] is None


EXISTING_SCHEMA = (
    "product_id string, product_name string, category string, price double, "
    "supplier string, effective_date date, end_date date, is_current boolean"
)


def test_changed_product_expires_old_and_inserts_new(spark):
    existing = spark.createDataFrame(
        [("P0001", "Product 1", "Electronics", 100.0, "Acme Corp", date(2025, 1, 1), None, True)],
        EXISTING_SCHEMA,
    )
    incoming = spark.createDataFrame(
        [("P0001", "Product 1", "Electronics", 150.0, "Acme Corp")],
        ["product_id", "product_name", "category", "price", "supplier"],
    )

    to_expire, to_insert = build_scd2_changes(existing, incoming, run_date=date(2026, 1, 1))

    expired = to_expire.collect()
    assert len(expired) == 1
    assert expired[0]["product_id"] == "P0001"
    assert expired[0]["end_date"] == date(2025, 12, 31)

    inserted = to_insert.collect()
    assert len(inserted) == 1
    assert inserted[0]["price"] == 150.0
    assert inserted[0]["effective_date"] == date(2026, 1, 1)


def test_unchanged_product_is_ignored(spark):
    existing = spark.createDataFrame(
        [("P0001", "Product 1", "Electronics", 100.0, "Acme Corp", date(2025, 1, 1), None, True)],
        EXISTING_SCHEMA,
    )
    incoming = spark.createDataFrame(
        [("P0001", "Product 1", "Electronics", 100.0, "Acme Corp")],
        ["product_id", "product_name", "category", "price", "supplier"],
    )

    to_expire, to_insert = build_scd2_changes(existing, incoming, run_date=date(2026, 1, 1))

    assert to_expire.count() == 0
    assert to_insert.count() == 0
