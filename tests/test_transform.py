from datetime import date

import pytest
from pyspark.sql import SparkSession

from scd2_pipeline.transform import build_scd2_changes

EXISTING_SCHEMA = (
    "product_id string, product_name string, category string, price double, "
    "supplier string, effective_date date, end_date date, is_current boolean"
)
INCOMING_COLUMNS = ["product_id", "product_name", "category", "price", "supplier"]


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
    yield session
    session.stop()


def test_new_product_is_inserted(spark):
    existing = spark.createDataFrame([], EXISTING_SCHEMA)
    incoming = spark.createDataFrame(
        [("P0001", "Product 1", "Electronics", 100.0, "Acme Corp")],
        INCOMING_COLUMNS,
    )

    to_expire, to_insert = build_scd2_changes(existing, incoming, run_date=date(2026, 1, 1))

    assert to_expire.count() == 0
    inserted = to_insert.collect()
    assert len(inserted) == 1
    assert inserted[0]["product_id"] == "P0001"
    assert inserted[0]["effective_date"] == date(2026, 1, 1)
    assert inserted[0]["end_date"] is None


def test_changed_product_expires_old_and_inserts_new(spark):
    existing = spark.createDataFrame(
        [("P0001", "Product 1", "Electronics", 100.0, "Acme Corp", date(2025, 1, 1), None, True)],
        EXISTING_SCHEMA,
    )
    incoming = spark.createDataFrame(
        [("P0001", "Product 1", "Electronics", 150.0, "Acme Corp")],
        INCOMING_COLUMNS,
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
        INCOMING_COLUMNS,
    )

    to_expire, to_insert = build_scd2_changes(existing, incoming, run_date=date(2026, 1, 1))

    assert to_expire.count() == 0
    assert to_insert.count() == 0


def test_mixed_snapshot_handles_new_changed_and_unchanged(spark):
    existing = spark.createDataFrame(
        [
            ("P0001", "Product 1", "Electronics", 100.0, "Acme Corp", date(2025, 1, 1), None, True),
            ("P0002", "Product 2", "Garden", 20.0, "Green Ltd", date(2025, 1, 1), None, True),
        ],
        EXISTING_SCHEMA,
    )
    incoming = spark.createDataFrame(
        [
            ("P0001", "Product 1", "Electronics", 100.0, "Acme Corp"),
            ("P0002", "Product 2", "Garden", 20.0, "Blue Inc"),
            ("P0003", "Product 3", "Toys", 35.0, "Fun Co"),
        ],
        INCOMING_COLUMNS,
    )

    to_expire, to_insert = build_scd2_changes(existing, incoming, run_date=date(2026, 1, 1))

    assert [r["product_id"] for r in to_expire.collect()] == ["P0002"]
    assert sorted(r["product_id"] for r in to_insert.collect()) == ["P0002", "P0003"]


def test_only_current_rows_are_compared(spark):
    existing = spark.createDataFrame(
        [
            (
                "P0001",
                "Product 1",
                "Electronics",
                80.0,
                "Acme Corp",
                date(2024, 1, 1),
                date(2024, 12, 31),
                False,
            ),
            ("P0001", "Product 1", "Electronics", 100.0, "Acme Corp", date(2025, 1, 1), None, True),
        ],
        EXISTING_SCHEMA,
    )
    incoming = spark.createDataFrame(
        [("P0001", "Product 1", "Electronics", 100.0, "Acme Corp")],
        INCOMING_COLUMNS,
    )

    to_expire, to_insert = build_scd2_changes(existing, incoming, run_date=date(2026, 1, 1))

    assert to_expire.count() == 0
    assert to_insert.count() == 0
