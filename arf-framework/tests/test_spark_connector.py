"""The 'spark' connection type (local Spark + Delta, used by the Docker lab).

pyspark is not needed: a fake SparkSession backed by SQLite stands in for it, so the
Spark connector's own code - schema reading, query execution, row conversion - runs
for real, end to end through the runner, on the demo data.
"""

import csv
import hashlib
import importlib.util
import re
import sqlite3

import pytest

from arf.cli import execute
from arf.config import Endpoint, load_settings, load_tables
from arf.connectors import TableNotFoundError
from arf.connectors import spark as spark_module
from arf.connectors.bigquery import BigQueryConnector
from arf.connectors.spark import SparkConnector, register_delta_tables
from arf.demo import DEMO_CONFIG, build_demo_databases
from arf.hashing import canonical_text
from conftest import write_yaml


class FakeType:
    def __init__(self, text):
        self.text = text

    def simpleString(self):
        return self.text


class FakeField:
    def __init__(self, name, type_text, nullable):
        self.name, self.dataType, self.nullable = name, FakeType(type_text), nullable


class FakeRow:
    def __init__(self, data):
        self.data = data

    def asDict(self):
        return dict(self.data)


def _format_string(fmt, value):
    """Spark's format_string('%.Nf', x): Java formats the exact decimal value, rounding half up."""
    digits = int(re.fullmatch(r"%\.(\d+)f", fmt).group(1))
    return canonical_text(value, "DECIMAL", digits)


def _date_format(value, pattern):
    if pattern == "yyyy-MM-dd":
        return canonical_text(value, "DATE")
    assert pattern == "yyyy-MM-dd HH:mm:ss.SSSSSS", pattern
    return canonical_text(value, "TIMESTAMP")


class FakeSpark:
    """Answers spark.sql() and spark.table() from a SQLite file declared with Spark type names.

    The Spark SQL functions ARF's row hash uses (md5, conv, pmod, concat, chr, format_string,
    date_format) are re-created in Python, so the Databricks/Spark hash SQL runs for real and its
    hashes must match the SQLite target's byte for byte.
    """

    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.conn.create_function("md5", 1, lambda s: hashlib.md5(str(s).encode("utf-8")).hexdigest())
        self.conn.create_function("conv", 3, lambda s, src, dst: str(int(s, src)))
        self.conn.create_function("pmod", 2, lambda a, n: a % n)
        self.conn.create_function("concat", -1, lambda *parts: "".join(str(p) for p in parts))
        self.conn.create_function("chr", 1, chr)
        self.conn.create_function("format_string", 2, _format_string)
        self.conn.create_function("date_format", 2, _date_format)
        self.statements = []
        self.stopped = False

    def sql(self, query):
        self.statements.append(query)
        # SQLite's closest equivalent of Spark's CAST(... AS STRING) is CAST(... AS TEXT).
        cur = self.conn.execute(query.replace(" AS STRING)", " AS TEXT)"))
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = [FakeRow(dict(zip(cols, r))) for r in cur.fetchall()]
        return type("DF", (), {"collect": lambda _self: rows})()

    def table(self, name):
        info = self.conn.execute(f'PRAGMA table_info("{name.split(".")[-1]}")').fetchall()
        if not info:
            raise RuntimeError(f"[TABLE_OR_VIEW_NOT_FOUND] The table or view `{name}` cannot be found.")
        fields = [FakeField(n, t.lower(), not notnull) for _cid, n, t, notnull, _d, _pk in info]
        return type("T", (), {"schema": type("S", (), {"fields": fields})()})()

    def stop(self):
        self.stopped = True


@pytest.fixture
def fake_spark(tmp_path, monkeypatch):
    build_demo_databases(tmp_path)
    holder = {}

    def fake_build(cfg):
        holder["spark"] = FakeSpark(tmp_path / "legacy_databricks.db")
        return holder["spark"]

    monkeypatch.setattr(spark_module, "build_spark_session", fake_build)
    return holder


def test_schema_types_and_nullability(fake_spark):
    conn = SparkConnector("legacy", {})
    cols = {c.name: c for c in conn.get_schema(Endpoint("legacy", "orders"))}
    assert cols["order_id"].family == "INTEGER" and cols["order_id"].nullable is False
    assert (cols["unit_price"].family, cols["unit_price"].precision, cols["unit_price"].scale) == ("DECIMAL", 38, 12)
    assert cols["order_ts"].family == "TIMESTAMP"
    with pytest.raises(TableNotFoundError):
        conn.get_schema(Endpoint("legacy", "legacy_sales.nope"))
    conn.close()
    assert fake_spark["spark"].stopped


def test_sql_is_the_databricks_sql(fake_spark):
    conn = SparkConnector("legacy", {})
    ep = Endpoint("legacy", "legacy_sales.orders", as_of_version=1)
    assert conn.table_ref(ep) == "`legacy_sales`.`orders` VERSION AS OF 1"
    assert conn.sum_expr("`qty`", "INTEGER") == "SUM(CAST(`qty` AS DECIMAL(38,0)))"


def test_full_run_with_spark_as_the_legacy_side(fake_spark, tmp_path):
    settings_path = write_yaml(tmp_path / "settings.yaml", {
        "connections": {
            "legacy_databricks": {"type": "spark", "delta_root": str(tmp_path / "delta")},
            "target_bigquery": {"type": "sqlite", "path": str(tmp_path / "target_bigquery.db"),
                                "emulate": "bigquery"},
        },
        "output": {"sinks": ["csv"], "output_dir": str(tmp_path / "runs")},
    })
    settings = load_settings(settings_path)
    summary = execute(settings, load_tables(DEMO_CONFIG / "tables", settings), ["csv"], run_id="spark_run")
    assert summary.table_status == {"customers": "PASS", "inventory": "ERROR", "orders": "FAIL"}
    assert any("COUNT(*)" in s for s in fake_spark["spark"].statements)

    # Spark-dialect hashes (source) and SQLite hashes (target) agree on every unchanged row,
    # so exactly the planted row-level defects are reported.
    with open(tmp_path / "runs" / "spark_run" / "arf_results.csv", encoding="utf-8-sig") as fh:
        rows = [r for r in csv.DictReader(fh) if r["table_id"] == "orders" and r["check_type"] == "row_hash"]
    changed = {r["column_name"] for r in rows if r["check_name"] == "changed_row"}
    missing = {r["column_name"] for r in rows if r["check_name"] == "missing_row"}
    assert missing == {"order_id=101", "order_id=202", "order_id=303"}
    assert changed == {f"order_id={i}" for i in (11, 22, 33, 44, 500, 600, 750, 900)}
    assert any("md5(" in s for s in fake_spark["spark"].statements)


def test_register_delta_tables(tmp_path):
    for table in ("orders", "customers"):
        (tmp_path / "legacy_sales" / table / "_delta_log").mkdir(parents=True)
    (tmp_path / "legacy_sales" / "not_delta").mkdir()            # no _delta_log -> ignored

    class Recorder:
        def __init__(self):
            self.statements = []

        def sql(self, q):
            self.statements.append(q)

    rec = Recorder()
    assert register_delta_tables(rec, tmp_path) == ["legacy_sales.customers", "legacy_sales.orders"]
    assert any("CREATE TABLE IF NOT EXISTS `legacy_sales`.`orders` USING DELTA LOCATION" in s
               for s in rec.statements)
    assert register_delta_tables(rec, tmp_path / "missing") == []


@pytest.mark.skipif(importlib.util.find_spec("pyspark") is not None, reason="pyspark is installed here")
def test_clear_message_without_pyspark():
    with pytest.raises(ImportError, match="arf-local-lab"):
        spark_module.build_spark_session({})


def test_bigquery_sum_cast_option():
    def conn(cfg):
        c = BigQueryConnector.__new__(BigQueryConnector)
        c.name, c.cfg = "bq", cfg
        return c
    assert conn({}).sum_expr("`q`", "INTEGER") == "SUM(CAST(`q` AS BIGNUMERIC))"
    assert conn({"sum_cast": "NUMERIC"}).sum_expr("`q`", "DECIMAL") == "SUM(CAST(`q` AS NUMERIC))"
    assert conn({"sum_cast": "none"}).sum_expr("`q`", "INTEGER") == "SUM(`q`)"
    assert conn({}).sum_expr("`f`", "FLOAT") == "SUM(`f`)"
