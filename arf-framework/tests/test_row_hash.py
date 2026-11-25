"""Row hash validation: canonical text, generated SQL, column planning and end-to-end results."""

import csv
import sqlite3
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from arf.cli import execute
from arf.compare.row_hash import plan_hash
from arf.config import ConfigError, Endpoint, load_settings, load_tables
from arf.connectors import HashColumn
from arf.connectors.bigquery import BigQueryConnector
from arf.connectors.databricks import DatabricksConnector
from arf.connectors.spark import SparkConnector
from arf.demo import DEMO_CONFIG, build_demo_databases
from arf.hashing import NULL_TOKEN, canonical_text
from conftest import bq, dbx, write_yaml


# ---- canonical text ---------------------------------------------------------------

@pytest.mark.parametrize("value, family, kwargs, expected", [
    (None, "STRING", {}, NULL_TOKEN),
    ("  Ann ", "STRING", {}, "  Ann "),
    ("  Ann ", "STRING", {"trim": True}, "Ann"),
    (1001, "INTEGER", {}, "1001"),
    ("42", "INTEGER", {}, "42"),
    (1, "BOOLEAN", {}, "true"),
    (Decimal("149.5"), "DECIMAL", {"scale": 2}, "149.50"),
    (149.505, "DECIMAL", {"scale": 2}, "149.51"),           # half rounds away from zero
    (Decimal("7"), "DECIMAL", {"scale": 0}, "7"),
    (0.1234567, "FLOAT", {}, "0.123457"),
    (12345.670000001, "FLOAT", {}, "12345.670000"),           # float noise disappears
    (date(2026, 1, 5), "DATE", {}, "2026-01-05"),
    ("2026-01-05T04:30:00Z", "TIMESTAMP", {}, "2026-01-05 04:30:00.000000"),
    (datetime(2026, 1, 5, 4, 30, tzinfo=timezone.utc), "TIMESTAMP", {}, "2026-01-05 04:30:00.000000"),
    ("2026-01-05 10:00:00", "DATETIME", {}, "2026-01-05 10:00:00.000000"),
])
def test_canonical_text(value, family, kwargs, expected):
    assert canonical_text(value, family, **kwargs) == expected


def test_same_value_in_different_formats_gives_same_text():
    assert canonical_text("2026-01-05 04:30:00", "TIMESTAMP") == canonical_text("2026-01-05T04:30:00Z", "TIMESTAMP")
    assert canonical_text(Decimal("149.50"), "DECIMAL", 2) == canonical_text(149.5, "DECIMAL", 2)


# ---- generated SQL ----------------------------------------------------------------

def bare(cls):
    obj = cls.__new__(cls)
    obj.name, obj.cfg = "test", {}
    return obj


KEYS = [HashColumn("order_id", "INTEGER")]
COLS = [HashColumn("amount", "DECIMAL", scale=2), HashColumn("order_ts", "TIMESTAMP"),
        HashColumn("discount", "FLOAT", float_digits=6)]


def test_databricks_hash_sql():
    sql = bare(DatabricksConnector).hash_bucket_sql(Endpoint("x", "main.sales.orders"), KEYS, COLS, 256)
    for piece in ("md5(", "pmod(", "conv(", "chr(31)", "format_string('%.2f', `amount`)",
                  "date_format(`order_ts`, 'yyyy-MM-dd HH:mm:ss.SSSSSS')", "format_string('%.6f', `discount`)",
                  "CASE WHEN `amount` IS NULL THEN '~NULL~'", "GROUP BY arf_bucket", "FROM `main`.`sales`.`orders`"):
        assert piece in sql, piece


def test_bigquery_hash_sql():
    sql = bare(BigQueryConnector).hash_bucket_sql(Endpoint("x", "p.d.orders"), KEYS, COLS, 256)
    for piece in ("TO_HEX(MD5(", "MOD(", "CAST(CONCAT('0x', ", "CODE_POINTS_TO_STRING([31])",
                  "FORMAT('%.2f', `amount`)", "FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S', `order_ts`, 'UTC')",
                  "GROUP BY arf_bucket", "FROM `p.d.orders`"):
        assert piece in sql, piece


def test_spark_uses_the_databricks_hash_sql():
    assert SparkConnector.hash_canonical is DatabricksConnector.hash_canonical


def test_literals_are_escaped():
    assert bare(DatabricksConnector).literal("O'Brien") == "'O\\'Brien'"
    assert bare(BigQueryConnector).literal("a\\b'c") == "'a\\\\b\\'c'"


# ---- which columns are hashed -------------------------------------------------------

SOURCE = [dbx("order_id", "BIGINT", False), dbx("amount", "DECIMAL(38,12)"), dbx("status", "STRING"),
          dbx("qty", "INT"), dbx("tags", "ARRAY<STRING>"), dbx("load_note", "STRING")]
TARGET = [bq("order_id", "INT64"), bq("amount", "NUMERIC"), bq("status", "STRING"),
          bq("qty", "STRING"), bq("tags", "ARRAY<STRING>"), bq("load_note", "STRING")]


def test_plan_picks_comparable_columns(make_table):
    plan = plan_hash(make_table(primary_key=["order_id"], hash={"exclude_columns": ["load_note"]}), SOURCE, TARGET)
    assert [k[0].name for k in plan.keys] == ["order_id"]
    assert [(c[0].name, c[2]) for c in plan.columns] == [("amount", 9), ("status", 0)]   # scale = min(12, 9)
    reasons = dict(plan.skipped)
    assert "INTEGER vs STRING" in reasons["qty"]
    assert "ARRAY" in reasons["tags"]
    assert "exclude_columns" in reasons["load_note"]


def test_plan_explicit_columns_and_scale(make_table):
    plan = plan_hash(make_table(primary_key=["order_id"], hash={"columns": ["amount", "nope"], "decimal_scale": 2}),
                     SOURCE, TARGET)
    assert [(c[0].name, c[2]) for c in plan.columns] == [("amount", 2)]
    assert dict(plan.skipped)["nope"].startswith("listed in hash.columns")


def test_plan_rejects_bad_keys(make_table):
    with pytest.raises(ValueError, match="not found"):
        plan_hash(make_table(primary_key=["missing_id"]), SOURCE, TARGET)
    with pytest.raises(ValueError, match="same type"):
        plan_hash(make_table(primary_key=["qty"]), SOURCE, TARGET)


def test_config_validation(settings, tmp_path):
    base = {"table_id": "t", "source": {"connection": "legacy_databricks", "table": "t"},
            "target": {"connection": "target_bigquery", "table": "t"}}
    write_yaml(tmp_path / "a.yaml", {**base, "checks": ["row_hash"]})
    with pytest.raises(ConfigError, match="primary_key"):
        load_tables(tmp_path / "a.yaml", settings)
    write_yaml(tmp_path / "b.yaml", {**base, "primary_key": "id", "hash": {"bucket": 10}})
    with pytest.raises(ConfigError, match="bucket"):
        load_tables(tmp_path / "b.yaml", settings)
    write_yaml(tmp_path / "c.yaml", {**base, "primary_key": "id", "hash": {"buckets": 0}})
    with pytest.raises(ConfigError, match="buckets"):
        load_tables(tmp_path / "c.yaml", settings)
    write_yaml(tmp_path / "d.yaml", {**base, "primary_key": "ID", "checks": ["row_hash"]})
    [t] = load_tables(tmp_path / "d.yaml", settings)
    assert t.primary_key == ["id"] and t.hash.buckets == 256


# ---- end to end on SQLite -----------------------------------------------------------

def run_tables(tmp_path, settings_file, tables_path, run_id):
    settings = load_settings(settings_file)
    summary = execute(settings, load_tables(tables_path, settings), ["csv"], run_id=run_id)
    with open(tmp_path / "runs" / run_id / "arf_results.csv", encoding="utf-8-sig") as fh:
        return summary, list(csv.DictReader(fh))


def hash_rows(results, table, check):
    return {r["column_name"]: r for r in results
            if r["table_id"] == table and r["check_type"] == "row_hash" and r["check_name"] == check}


def test_demo_orders_every_row_level_defect_is_named(tmp_path, settings_file):
    build_demo_databases(tmp_path)
    summary, results = run_tables(tmp_path, settings_file, DEMO_CONFIG / "tables", "hash_demo")
    assert summary.table_status == {"customers": "PASS", "inventory": "ERROR", "orders": "FAIL"}

    assert set(hash_rows(results, "orders", "missing_row")) == {"order_id=101", "order_id=202", "order_id=303"}
    assert not hash_rows(results, "orders", "extra_row")
    changed = hash_rows(results, "orders", "changed_row")
    assert set(changed) == {f"order_id={i}" for i in (11, 22, 33, 44, 500, 600, 750, 900)}
    assert changed["order_id=600"]["message"] == "changed column(s): status"
    assert changed["order_id=600"]["source_value"].startswith("status=")
    assert changed["order_id=11"]["target_value"] == "customer_id=NULL"
    assert changed["order_id=500"]["source_value"] == "amount=763.28"
    assert changed["order_id=500"]["target_value"] == "amount=768.28"

    customers = [r for r in results if r["table_id"] == "customers" and r["check_type"] == "row_hash"]
    assert {r["check_name"]: r["status"] for r in customers} == {"hash_columns": "SKIPPED", "bucket_summary": "PASS"}


def make_pair(tmp_path, src_rows, tgt_rows):
    """Two small SQLite tables, declared with Databricks and BigQuery types."""
    for name, ddl, rows in (
            ("legacy_databricks", "CREATE TABLE t (id BIGINT, region STRING, status STRING, amount DECIMAL(18,2))",
             src_rows),
            ("target_bigquery", "CREATE TABLE t (id INT64, region STRING, status STRING, amount NUMERIC)",
             tgt_rows)):
        conn = sqlite3.connect(tmp_path / f"{name}.db")
        conn.execute(ddl)
        conn.executemany("INSERT INTO t VALUES (?, ?, ?, ?)", rows)
        conn.commit()
        conn.close()


def table_yaml(tmp_path, **extra):
    return write_yaml(tmp_path / "t.yaml", {
        "table_id": "t", "checks": ["schema", "row_count", "numeric", "row_hash"], "primary_key": ["id"],
        "source": {"connection": "legacy_databricks", "table": "t"},
        "target": {"connection": "target_bigquery", "table": "t"}, **extra})


def test_only_the_hash_catches_swapped_values(tmp_path, settings_file):
    rows = [(i, "N" if i % 2 else "S", "NEW" if i % 3 else "SHIPPED", Decimal(i) + Decimal("0.5")) for i in range(1, 201)]
    rows = [(i, r, s, float(a)) for i, r, s, a in rows]
    swapped = list(rows)
    swapped[2] = (3, rows[2][1], rows[3][2], rows[2][3])     # statuses of rows 3 and 4 swapped:
    swapped[3] = (4, rows[3][1], rows[2][2], rows[3][3])     # counts per status stay identical
    make_pair(tmp_path, rows, swapped)

    summary, results = run_tables(tmp_path, settings_file, table_yaml(tmp_path), "swap")
    others = [r for r in results if r["check_type"] != "row_hash"]
    assert others and all(r["status"] == "PASS" for r in others)        # schema, counts, SUM/MIN/MAX all pass
    changed = hash_rows(results, "t", "changed_row")
    assert set(changed) == {"id=3", "id=4"}
    assert changed["id=3"]["source_value"] == "status=SHIPPED" and changed["id=3"]["target_value"] == "status=NEW"
    assert summary.exit_code == 1


def test_extra_rows_composite_keys_and_quotes(tmp_path, settings_file):
    src = [(1, "N", "NEW", 1.0), (2, "S", "NEW", 2.0)]
    tgt = src + [(2, "O'Brien", "NEW", 3.0)]                 # same id, new region: a new composite key
    make_pair(tmp_path, src, tgt)
    _, results = run_tables(tmp_path, settings_file, table_yaml(tmp_path, primary_key=["id", "region"]), "extra")
    assert set(hash_rows(results, "t", "extra_row")) == {"id=2, region=O'Brien"}


def test_limits_are_reported(tmp_path, settings_file):
    rows = [(i, "N", "NEW", float(i)) for i in range(1, 501)]
    tgt = [(i, "N", "LATE" if i % 50 == 0 else "NEW", float(i)) for i in range(1, 501)]   # 10 changed rows
    make_pair(tmp_path, rows, tgt)
    yaml_path = table_yaml(tmp_path, hash={"buckets": 64, "max_buckets_to_inspect": 2, "max_examples": 1})
    _, results = run_tables(tmp_path, settings_file, yaml_path, "limits")
    summary = next(iter(hash_rows(results, "t", "row_differences").values()))
    assert "not inspected" in summary["message"]
    assert len(hash_rows(results, "t", "changed_row")) == 1
