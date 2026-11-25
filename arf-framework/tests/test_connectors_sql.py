"""Checks the SQL each connector generates and how it reads metadata - without installing
databricks-sql-connector or google-cloud-bigquery (fake objects stand in for them)."""

import pytest

from arf.config import Endpoint
from arf.connectors import MetricSpec, TableNotFoundError
from arf.connectors.bigquery import BigQueryConnector
from arf.connectors.databricks import DatabricksConnector


def bare(cls):
    """A connector object without opening a real connection."""
    obj = cls.__new__(cls)
    obj.name, obj.cfg = "test", {}
    return obj


SPECS = [MetricSpec("m0", "sum", "qty", "INTEGER"), MetricSpec("m1", "sum", "amount", "DECIMAL"),
         MetricSpec("m2", "max", "order ts", "TIMESTAMP"), MetricSpec("m3", "null_count", "name", "STRING")]


def test_databricks_sql():
    c = bare(DatabricksConnector)
    ep = Endpoint("x", "main.sales.orders", filter="order_date >= '2026-01-01'", as_of_version=12)
    sql = c.profile_sql(ep, SPECS)
    assert "FROM `main`.`sales`.`orders` VERSION AS OF 12 WHERE (order_date >= '2026-01-01')" in sql
    assert "SUM(CAST(`qty` AS DECIMAL(38,0))) AS m0" in sql     # BIGINT overflow guard
    assert "SUM(`amount`) AS m1" in sql
    assert "MAX(`order ts`) AS m2" in sql                       # names with spaces are quoted
    assert "COUNT(*) - COUNT(`name`) AS m3" in sql


def test_databricks_time_travel_by_timestamp():
    c = bare(DatabricksConnector)
    assert c.table_ref(Endpoint("x", "s.t", as_of_timestamp="2026-11-20")) == \
        "`s`.`t` TIMESTAMP AS OF '2026-11-20'"


def test_bigquery_sql():
    c = bare(BigQueryConnector)
    ep = Endpoint("x", "proj.ds.orders", filter="order_date >= '2026-01-01'",
                  as_of_timestamp="2026-11-20 00:00:00")
    sql = c.profile_sql(ep, SPECS)
    assert "FROM `proj.ds.orders` FOR SYSTEM_TIME AS OF TIMESTAMP '2026-11-20 00:00:00'" in sql
    assert "SUM(CAST(`qty` AS BIGNUMERIC)) AS m0" in sql
    assert "SUM(CAST(`amount` AS BIGNUMERIC)) AS m1" in sql
    with pytest.raises(ValueError):
        c.table_ref(Endpoint("x", "p.d.t", as_of_version=3))


# ---- Databricks metadata parsing with a fake cursor -------------------------

class FakeCursor:
    def __init__(self, rows):
        self.description = [(k,) for k in rows[0]] if rows else []
        self._rows = [tuple(r.values()) for r in rows]

    def columns(self, **kwargs):
        pass

    def fetchall(self):
        return self._rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConn:
    def __init__(self, rows):
        self.rows = rows

    def cursor(self):
        return FakeCursor(self.rows)


def meta(name, type_name, size=None, digits=None, nullable="YES", pos=1, table="order_items", schema="sales"):
    return {"TABLE_CAT": "main", "TABLE_SCHEM": schema, "TABLE_NAME": table, "COLUMN_NAME": name,
            "TYPE_NAME": type_name, "COLUMN_SIZE": size, "DECIMAL_DIGITS": digits,
            "IS_NULLABLE": nullable, "ORDINAL_POSITION": pos}


def test_databricks_metadata_parsing():
    c = bare(DatabricksConnector)
    c.conn = FakeConn([
        meta("amount", "DECIMAL", 18, 2, pos=2),
        meta("id", "BIGINT", nullable="NO", pos=1),
        meta("x", "INT", table="orderXitems"),     # matched by the '_' wildcard; must be ignored
    ])
    cols = c.get_schema(Endpoint("x", "main.sales.order_items"))
    assert [col.name for col in cols] == ["id", "amount"]
    assert cols[0].nullable is False and cols[0].family == "INTEGER"
    assert (cols[1].raw_type, cols[1].precision, cols[1].scale) == ("DECIMAL(18,2)", 18, 2)


def test_bigquery_schema_and_not_found():
    class Field:
        def __init__(self, name, field_type, mode="NULLABLE", precision=None, scale=None):
            self.name, self.field_type, self.mode, self.precision, self.scale = name, field_type, mode, precision, scale

    class NotFound(Exception):
        pass

    class FakeClient:
        def get_table(self, name):
            if name == "p.d.missing":
                raise NotFound("404")
            return type("T", (), {"schema": [Field("id", "INTEGER", "REQUIRED"), Field("amt", "NUMERIC", precision=12, scale=2),
                                             Field("tags", "STRING", "REPEATED")]})()

    c = bare(BigQueryConnector)
    c.client = FakeClient()
    cols = c.get_schema(Endpoint("x", "p.d.t"))
    assert [(x.name, x.family, x.nullable) for x in cols] == [
        ("id", "INTEGER", False), ("amt", "DECIMAL", True), ("tags", "ARRAY", True)]
    assert (cols[1].precision, cols[1].scale) == (12, 2)
    with pytest.raises(TableNotFoundError):
        c.get_schema(Endpoint("x", "p.d.missing"))
