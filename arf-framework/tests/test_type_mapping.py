import pytest

from arf.type_mapping import (ARRAY, BINARY, BOOLEAN, DATETIME, DECIMAL, FLOAT, INTEGER, STRING, STRUCT,
                              TIMESTAMP, UNKNOWN, is_compatible, parse_type)


@pytest.mark.parametrize("raw, family", [
    ("BIGINT", INTEGER), ("int", INTEGER), ("SMALLINT", INTEGER), ("DOUBLE", FLOAT), ("FLOAT", FLOAT),
    ("STRING", STRING), ("VARCHAR(100)", STRING), ("BOOLEAN", BOOLEAN), ("BINARY", BINARY),
    ("TIMESTAMP", TIMESTAMP), ("TIMESTAMP_NTZ", DATETIME), ("ARRAY<STRING>", ARRAY),
    ("STRUCT<a:INT,b:STRING>", STRUCT), ("GEOMETRY", UNKNOWN),
])
def test_databricks_families(raw, family):
    assert parse_type(raw, "databricks")[0] == family


@pytest.mark.parametrize("raw, family", [
    ("INT64", INTEGER), ("FLOAT64", FLOAT), ("BOOL", BOOLEAN), ("BYTES", BINARY), ("RECORD", STRUCT),
    ("DATETIME", DATETIME), ("TIMESTAMP", TIMESTAMP), ("ARRAY<INT64>", ARRAY),
])
def test_bigquery_families(raw, family):
    assert parse_type(raw, "bigquery")[0] == family


@pytest.mark.parametrize("raw, dialect, expected", [
    ("DECIMAL(18,2)", "databricks", (DECIMAL, 18, 2)),
    ("decimal(10)", "databricks", (DECIMAL, 10, 0)),
    ("DECIMAL", "databricks", (DECIMAL, 10, 0)),          # Spark default
    ("NUMERIC", "bigquery", (DECIMAL, 38, 9)),            # BigQuery default
    ("BIGNUMERIC", "bigquery", (DECIMAL, 76, 38)),
    ("NUMERIC(12,4)", "bigquery", (DECIMAL, 12, 4)),
])
def test_decimal_precision_and_scale(raw, dialect, expected):
    assert parse_type(raw, dialect) == expected


def test_unknown_dialect_is_rejected():
    with pytest.raises(ValueError):
        parse_type("INT", "oracle")


def test_compatibility():
    assert is_compatible(INTEGER, INTEGER)
    assert not is_compatible(INTEGER, STRING)
    assert is_compatible("MAP", "JSON", [["MAP", "JSON"]])
    assert not is_compatible("JSON", "MAP", [["MAP", "JSON"]])   # direction matters
