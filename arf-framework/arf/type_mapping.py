"""Normalise Databricks (Spark SQL) and BigQuery type names into one canonical vocabulary.

Databricks says BIGINT, BigQuery says INT64 - both are family INTEGER, so the
schema check treats them as aligned. Anything that lands in a different family
(e.g. INT -> STRING) is reported as a type mismatch.
"""

from __future__ import annotations

import re

INTEGER = "INTEGER"
FLOAT = "FLOAT"
DECIMAL = "DECIMAL"
STRING = "STRING"
BOOLEAN = "BOOLEAN"
BINARY = "BINARY"
DATE = "DATE"
TIMESTAMP = "TIMESTAMP"   # absolute instant (Databricks TIMESTAMP / TIMESTAMP_LTZ, BigQuery TIMESTAMP)
DATETIME = "DATETIME"     # wall-clock time without zone (Databricks TIMESTAMP_NTZ, BigQuery DATETIME)
TIME = "TIME"
ARRAY = "ARRAY"
STRUCT = "STRUCT"
MAP = "MAP"
JSON = "JSON"
GEOGRAPHY = "GEOGRAPHY"
INTERVAL = "INTERVAL"
UNKNOWN = "UNKNOWN"

NUMERIC_FAMILIES = {INTEGER, FLOAT, DECIMAL}
TEMPORAL_FAMILIES = {DATE, TIMESTAMP, DATETIME}
# NULL counts are skipped for these: some engines cannot COUNT() nested types.
COMPLEX_FAMILIES = {ARRAY, STRUCT, MAP, GEOGRAPHY, UNKNOWN}

_DATABRICKS = {
    "TINYINT": INTEGER, "BYTE": INTEGER, "SMALLINT": INTEGER, "SHORT": INTEGER,
    "INT": INTEGER, "INTEGER": INTEGER, "BIGINT": INTEGER, "LONG": INTEGER,
    "FLOAT": FLOAT, "REAL": FLOAT, "DOUBLE": FLOAT,
    "DECIMAL": DECIMAL, "DEC": DECIMAL, "NUMERIC": DECIMAL,
    "STRING": STRING, "VARCHAR": STRING, "CHAR": STRING,
    "BOOLEAN": BOOLEAN, "BINARY": BINARY, "DATE": DATE,
    "TIMESTAMP": TIMESTAMP, "TIMESTAMP_LTZ": TIMESTAMP, "TIMESTAMP_NTZ": DATETIME,
    "ARRAY": ARRAY, "STRUCT": STRUCT, "MAP": MAP, "VARIANT": JSON,
    "INTERVAL": INTERVAL, "VOID": UNKNOWN,
}

_BIGQUERY = {
    "INT64": INTEGER, "INTEGER": INTEGER, "INT": INTEGER, "SMALLINT": INTEGER,
    "BIGINT": INTEGER, "TINYINT": INTEGER, "BYTEINT": INTEGER,
    "FLOAT64": FLOAT, "FLOAT": FLOAT,
    "NUMERIC": DECIMAL, "DECIMAL": DECIMAL, "BIGNUMERIC": DECIMAL, "BIGDECIMAL": DECIMAL,
    "STRING": STRING, "BOOL": BOOLEAN, "BOOLEAN": BOOLEAN, "BYTES": BINARY,
    "DATE": DATE, "DATETIME": DATETIME, "TIMESTAMP": TIMESTAMP, "TIME": TIME,
    "ARRAY": ARRAY, "STRUCT": STRUCT, "RECORD": STRUCT, "JSON": JSON,
    "GEOGRAPHY": GEOGRAPHY, "INTERVAL": INTERVAL, "RANGE": UNKNOWN,
}

_DIALECTS = {"databricks": _DATABRICKS, "bigquery": _BIGQUERY}

# Default (precision, scale) when the engine reports a decimal without parameters.
_DEFAULT_DECIMAL = {
    ("databricks", "DECIMAL"): (10, 0), ("databricks", "DEC"): (10, 0), ("databricks", "NUMERIC"): (10, 0),
    ("bigquery", "NUMERIC"): (38, 9), ("bigquery", "DECIMAL"): (38, 9),
    ("bigquery", "BIGNUMERIC"): (76, 38), ("bigquery", "BIGDECIMAL"): (76, 38),
}

_BASE_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)")
_PARAMS_RE = re.compile(r"^\s*[A-Za-z_][A-Za-z0-9_]*\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)")


def parse_type(raw_type: str, dialect: str) -> tuple[str, int | None, int | None]:
    """Return (family, precision, scale) for a raw type string such as 'DECIMAL(18,2)' or 'ARRAY<STRING>'."""
    if dialect not in _DIALECTS:
        raise ValueError(f"Unknown type dialect '{dialect}'. Use one of: {', '.join(_DIALECTS)}")
    if not raw_type:
        return UNKNOWN, None, None

    text = raw_type.strip().upper()
    match = _BASE_RE.match(text)
    if not match:
        return UNKNOWN, None, None
    base = match.group(1)
    family = _DIALECTS[dialect].get(base, UNKNOWN)

    precision = scale = None
    if family == DECIMAL:
        params = _PARAMS_RE.match(text)
        if params:
            precision = int(params.group(1))
            scale = int(params.group(2)) if params.group(2) is not None else 0
        else:
            precision, scale = _DEFAULT_DECIMAL.get((dialect, base), (None, None))
    return family, precision, scale


def is_compatible(source_family: str, target_family: str, allowed_changes: list[list[str]] | None = None) -> bool:
    """Same family = compatible. Extra pairs (e.g. MAP -> JSON) can be allowed in settings.yaml."""
    if source_family == target_family:
        return True
    for pair in allowed_changes or []:
        if len(pair) == 2 and pair[0].upper() == source_family and pair[1].upper() == target_family:
            return True
    return False
