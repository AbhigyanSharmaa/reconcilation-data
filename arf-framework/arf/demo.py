"""`python -m arf demo` - builds two local SQLite databases that imitate the legacy
Databricks table and its migrated BigQuery copy, plants known defects, and runs ARF.

Use it to see the output before you have Databricks or BigQuery access, and to
show the framework working. The same function feeds the end-to-end unit test.
"""

from __future__ import annotations

import os
import random
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from .config import load_settings, load_tables

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEMO_CONFIG = PROJECT_ROOT / "config" / "demo"

# The planted defects. The Docker lab (arf-local-lab/notebooks/02_migration_job) imports
# these same sets, so the demo and the lab always expect the same ARF findings.
MISSING_ORDER_IDS = {101, 202, 303}          # rows dropped during "migration"
NULLED_CUSTOMER_IDS = {11, 22, 33, 44}       # customer_id lost on these orders
CHANGED_AMOUNT_IDS = {500, 750}              # amount drifted by +5.00
CHANGED_STATUS_IDS = {600, 900}              # status changed: no count or SUM/MIN/MAX can see this,
                                             # only the row_hash check catches it


def changed_status(status: str) -> str:
    """The wrong status the 'migration' writes for CHANGED_STATUS_IDS (always different from the original)."""
    return "NEW" if status == "CANCELLED" else "CANCELLED"

LEGACY_TS_FORMAT = "%Y-%m-%d %H:%M:%S"       # how the legacy side writes timestamps as text
IST_OFFSET = timedelta(hours=5, minutes=30)  # legacy customers.created_ts is IST wall-clock time


def _write(path: Path, statements: list[tuple[str, list]]) -> None:
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(path)
    for ddl, rows in statements:
        conn.execute(ddl)
        if rows:
            placeholders = ",".join("?" * len(rows[0]))
            table = ddl.split()[2]
            conn.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)
    conn.commit()
    conn.close()


def legacy_rows(seed: int = 42) -> dict[str, list[tuple]]:
    """The legacy (source) sample data, as plain Python tuples. Deterministic (seeded).

    Used by build_demo_databases() below and by the Docker lab's 01_legacy_pipeline notebook,
    so both start from identical rows.
      customers: (customer_id, customer_name, signup_date, created_ts IST text, credit_limit, lifetime_value)
      orders:    (order_id, cust_id, order_ts UTC text, quantity, unit_price, amount, discount, status, legacy_flag)
      inventory: (sku_id, sku_code, on_hand)
    """
    rng = random.Random(seed)

    customers = []
    base_ist = datetime(2025, 1, 1, 9, 0, 0)
    for cid in range(1, 301):
        created_ist = base_ist + timedelta(hours=cid * 7, minutes=rng.randint(0, 59))
        credit = round(rng.uniform(1000, 50000), 2)
        ltv = round(rng.uniform(0, 250000), 2)
        customers.append((cid, f"Customer {cid:04d}", created_ist.date().isoformat(),
                          created_ist.strftime(LEGACY_TS_FORMAT), credit, ltv))

    orders = []
    base_utc = datetime(2026, 1, 1, 0, 0, 0)
    for oid in range(1, 1001):
        ts = base_utc + timedelta(minutes=oid * 37)
        cust = rng.randint(1, 300)
        qty = rng.randint(1, 10)
        unit_price = round(rng.uniform(5, 500), 6)
        amount = round(qty * unit_price, 2)
        discount = round(rng.uniform(0, 0.3), 4)
        status = rng.choice(["NEW", "SHIPPED", "DELIVERED", "CANCELLED"])
        flag = rng.choice(["Y", "N"])
        orders.append((oid, cust, ts.strftime(LEGACY_TS_FORMAT), qty, unit_price, amount, discount, status, flag))

    inventory = [(sku, f"SKU-{sku:05d}", rng.randint(0, 500)) for sku in range(1, 51)]
    return {"customers": customers, "orders": orders, "inventory": inventory}


def build_demo_databases(out_dir: Path) -> tuple[Path, Path]:
    """Create legacy_databricks.db and target_bigquery.db in out_dir. Deterministic (seeded)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = legacy_rows()
    cust_src, ord_src, inventory = rows["customers"], rows["orders"], rows["inventory"]

    # customers: no real defects. Legacy stores IST wall-clock text, target stores UTC ISO-8601;
    # float columns carry 1e-9 noise. Everything should PASS thanks to tolerance settings.
    cust_tgt = []
    for cid, name, signup, created_ist_text, credit, ltv in cust_src:
        created_utc = datetime.strptime(created_ist_text, LEGACY_TS_FORMAT) - IST_OFFSET
        cust_tgt.append((cid, name, signup, created_utc.strftime("%Y-%m-%dT%H:%M:%SZ"), credit + 1e-9, ltv))

    # orders: several planted defects (see MISSING_/NULLED_/CHANGED_ sets and the schema changes).
    ord_tgt = []
    for oid, cust, ts_text, qty, unit_price, amount, discount, status, _flag in ord_src:
        if oid in MISSING_ORDER_IDS:
            continue
        ts = datetime.strptime(ts_text, LEGACY_TS_FORMAT)
        ord_tgt.append((
            oid,
            None if oid in NULLED_CUSTOMER_IDS else cust,
            ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            str(qty),
            unit_price,
            round(amount + 5.0, 2) if oid in CHANGED_AMOUNT_IDS else amount,
            discount,
            changed_status(status) if oid in CHANGED_STATUS_IDS else status,
            "2026-11-24T00:00:00Z",
        ))

    source_db = out_dir / "legacy_databricks.db"
    target_db = out_dir / "target_bigquery.db"
    _write(source_db, [
        ("CREATE TABLE customers (customer_id BIGINT NOT NULL, customer_name STRING, signup_date DATE, "
         "created_ts TIMESTAMP, credit_limit DOUBLE, lifetime_value DECIMAL(18,2))", cust_src),
        ("CREATE TABLE orders (order_id BIGINT NOT NULL, cust_id BIGINT, order_ts TIMESTAMP, quantity INT, "
         "unit_price DECIMAL(38,12), amount DECIMAL(18,2), discount DOUBLE, status STRING, legacy_flag STRING)",
         ord_src),
        ("CREATE TABLE inventory (sku_id BIGINT NOT NULL, sku_code STRING, on_hand INT)", inventory),
    ])
    _write(target_db, [
        ("CREATE TABLE customers (customer_id INT64 NOT NULL, customer_name STRING, signup_date DATE, "
         "created_ts TIMESTAMP, credit_limit FLOAT64, lifetime_value NUMERIC)", cust_tgt),
        ("CREATE TABLE orders (order_id INT64, customer_id INT64, order_ts TIMESTAMP, quantity STRING, "
         "unit_price NUMERIC, amount NUMERIC, discount FLOAT64, status STRING, load_ts TIMESTAMP)", ord_tgt),
        # inventory deliberately not created: the target table was never migrated.
    ])
    return source_db, target_db


EXPLANATION = """
What the demo planted, and what ARF should report:
  customers - no real defects. Timestamps differ only in format and time zone
              (IST text vs UTC ISO-8601) and floats differ by 1e-9.        Expected: PASS
  orders    - 3 rows missing, 2 amounts changed by +5.00, 4 customer_ids NULL,
              2 statuses changed (only row_hash can see this), legacy_flag dropped,
              quantity INT -> STRING, unit_price scale 12 -> 9, order_id lost
              NOT NULL, new column load_ts.                                Expected: FAIL
  inventory - target table was never created.                             Expected: ERROR
              (logged with traceback in arf.log, and the run carries on)
Open arf_exceptions.csv in Excel to see every WARN / FAIL / ERROR row. The row_hash rows
name each missing or changed order and show the old and new values.
"""


def run_demo(verbose: bool = False, out_root: Path | None = None) -> int:
    from .cli import execute

    out_dir = (out_root or Path("output")) / "demo"
    build_demo_databases(out_dir)
    os.environ["ARF_DEMO_DIR"] = str(out_dir.resolve())   # used by config/demo/settings.yaml
    settings = load_settings(DEMO_CONFIG / "settings.yaml")
    tables = load_tables(DEMO_CONFIG / "tables", settings)
    execute(settings, tables, verbose=verbose)
    print(EXPLANATION)
    return 0
