"""
load_and_run.py
---------------
Loads the synthetic CSVs into an in-memory SQLite database, runs query.sql,
and prints the results.

Run:  python load_and_run.py
"""

import csv
import sqlite3
from pathlib import Path

DATA_DIR = Path("data")
QUERY_PATH = Path("query.sql")


def load_csv(cursor, table_name, csv_path, column_types):
    """Create a table and bulk-load a CSV into it."""
    columns = ", ".join(f"{name} {sqltype}" for name, sqltype in column_types.items())
    cursor.execute(f"CREATE TABLE {table_name} ({columns})")

    with open(csv_path) as f:
        reader = csv.DictReader(f)
        rows = []
        for r in reader:
            row = []
            for col, sqltype in column_types.items():
                if sqltype == "INTEGER":
                    row.append(int(r[col]))
                elif sqltype == "REAL":
                    row.append(float(r[col]))
                else:
                    row.append(r[col])
            rows.append(tuple(row))

    placeholders = ", ".join("?" for _ in column_types)
    cursor.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", rows)
    return len(rows)


def main():
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()

    n_msas = load_csv(
        cur, "master_agreement", DATA_DIR / "master_agreement.csv",
        {"msa_id": "INTEGER", "msa_code": "TEXT", "client_name": "TEXT", "msa_ceiling_amount": "REAL"},
    )
    n_orders = load_csv(
        cur, "procurement_order", DATA_DIR / "procurement_order.csv",
        {"order_id": "INTEGER", "msa_id": "INTEGER", "procurement_type_id": "INTEGER",
         "order_code": "TEXT", "order_ceiling_amount": "REAL"},
    )
    print(f"Loaded {n_msas:,} MSAs and {n_orders:,} procurement orders.\n")

    query = QUERY_PATH.read_text()
    results = cur.execute(query).fetchall()

    print(f"{len(results)} MSAs in overage status:\n")
    print(f"  {'msa_code':<12} {'ceiling':>15} {'committed':>15} {'overage':>15}")
    print(f"  {'-'*12} {'-'*15} {'-'*15} {'-'*15}")
    for msa_code, ceiling, committed, _, overage in results[:10]:
        print(f"  {msa_code:<12} {ceiling:>15,.2f} {committed:>15,.2f} {overage:>15,.2f}")

    if len(results) > 10:
        print(f"  ... and {len(results) - 10} more")

    total = sum(r[4] for r in results)
    print(f"\nTotal overage across all MSAs: ${total:,.2f}")


if __name__ == "__main__":
    main()
