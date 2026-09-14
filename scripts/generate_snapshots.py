"""Generates two synthetic product catalog snapshots to demo the SCD2 pipeline.

products_day1.csv: the initial catalog.
products_day2.csv: same catalog with some price/category/supplier changes,
plus a few brand new products.
"""

import csv
import random
from pathlib import Path

random.seed(42)

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

CATEGORIES = ["Electronics", "Home", "Sports", "Toys", "Books"]
SUPPLIERS = ["Acme Corp", "Globex", "Initech", "Umbrella Supply", "Soylent Co"]

FIELDNAMES = ["product_id", "product_name", "category", "price", "supplier"]


def build_catalog(n: int) -> list[dict]:
    catalog = []
    for i in range(1, n + 1):
        catalog.append(
            {
                "product_id": f"P{i:04d}",
                "product_name": f"Product {i}",
                "category": random.choice(CATEGORIES),
                "price": round(random.uniform(5, 500), 2),
                "supplier": random.choice(SUPPLIERS),
            }
        )
    return catalog


def apply_changes(catalog: list[dict], change_ratio: float, new_products: int) -> list[dict]:
    updated = [row.copy() for row in catalog]
    changeable = random.sample(updated, k=int(len(updated) * change_ratio))
    for row in changeable:
        field = random.choice(["category", "price", "supplier"])
        if field == "price":
            row["price"] = round(row["price"] * random.uniform(0.8, 1.3), 2)
        else:
            options = CATEGORIES if field == "category" else SUPPLIERS
            row[field] = random.choice(options)

    start_id = len(catalog) + 1
    for i in range(start_id, start_id + new_products):
        updated.append(
            {
                "product_id": f"P{i:04d}",
                "product_name": f"Product {i}",
                "category": random.choice(CATEGORIES),
                "price": round(random.uniform(5, 500), 2),
                "supplier": random.choice(SUPPLIERS),
            }
        )
    return updated


def write_csv(rows: list[dict], path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    day1 = build_catalog(n=50)
    day2 = apply_changes(day1, change_ratio=0.2, new_products=5)

    write_csv(day1, OUTPUT_DIR / "products_day1.csv")
    write_csv(day2, OUTPUT_DIR / "products_day2.csv")

    print(f"Wrote {len(day1)} rows to products_day1.csv")
    print(f"Wrote {len(day2)} rows to products_day2.csv")


if __name__ == "__main__":
    main()
