"""Local SQLite connector - lets you build and unit-test ARF with no cloud access.

SQLite accepts any type name in CREATE TABLE, so a test table can be declared
with Databricks types (BIGINT, DECIMAL(18,2)) or BigQuery types (INT64, NUMERIC).
Set `emulate: databricks` or `emulate: bigquery` so the types are read with the
right vocabulary.

SQLite has no MD5 or number formatting, so for row hashing the connector registers
small Python functions (arf_md5, arf_hex_to_int, arf_canon) that apply the same
rules as arf/hashing.py.
"""

from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

from ..config import Endpoint
from ..hashing import HashColumn, canonical_text
from ..models import ColumnInfo
from .base import Connector, TableNotFoundError


def _md5(text):
    return None if text is None else hashlib.md5(str(text).encode("utf-8")).hexdigest()


def _hex_to_int(text):
    return None if text is None else int(text, 16)


def _canon(value, family, scale, float_digits, trim):
    return canonical_text(value, family, int(scale), int(float_digits), bool(trim))


class SQLiteConnector(Connector):
    def __init__(self, name: str, cfg: dict):
        super().__init__(name, cfg)
        path = cfg.get("path")
        if not path:
            raise ValueError(f"Connection '{name}': sqlite needs a 'path'")
        if not Path(path).exists():
            raise FileNotFoundError(f"Connection '{name}': SQLite file not found: {path}")
        self.type_dialect = cfg.get("emulate", "databricks")
        self.conn = sqlite3.connect(path)
        self.conn.create_function("arf_md5", 1, _md5, deterministic=True)
        self.conn.create_function("arf_hex_to_int", 1, _hex_to_int, deterministic=True)
        self.conn.create_function("arf_canon", 5, _canon, deterministic=True)

    def quote(self, identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    def table_ref(self, endpoint: Endpoint) -> str:
        if endpoint.as_of_version is not None or endpoint.as_of_timestamp:
            raise ValueError("Time travel (as_of_*) is not supported for sqlite connections")
        return ".".join(self.quote(part) for part in endpoint.table.split("."))

    def get_schema(self, endpoint: Endpoint) -> list[ColumnInfo]:
        rows = self.conn.execute(f"PRAGMA table_info({self.quote(endpoint.table)})").fetchall()
        if not rows:
            raise TableNotFoundError(f"Table '{endpoint.table}' not found in {self.cfg.get('path')}")
        # PRAGMA table_info -> (cid, name, type, notnull, default, pk)
        return [self.column(name, col_type, nullable=not notnull, ordinal=cid + 1)
                for cid, name, col_type, notnull, _default, _pk in rows]

    def _execute(self, sql: str) -> list[dict]:
        cur = self.conn.execute(sql)
        columns = [d[0] for d in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]

    # ---- row hash -------------------------------------------------------------
    def literal(self, text: str) -> str:
        return "'" + text.replace("'", "''") + "'"

    def hash_md5(self, expr: str) -> str:
        return f"arf_md5({expr})"

    def hash_hex_to_int(self, hex8_expr: str) -> str:
        return f"arf_hex_to_int({hex8_expr})"

    def hash_mod(self, expr: str, n: int) -> str:
        return f"({expr} % {n})"

    def hash_concat(self, parts: list[str]) -> str:
        return "(" + " || ".join(parts) + ")"

    def hash_separator(self) -> str:
        return "char(31)"

    def hash_canonical(self, quoted_column: str, col: HashColumn) -> str:
        return (f"arf_canon({quoted_column}, '{col.family}', {col.scale}, {col.float_digits}, "
                f"{1 if col.trim else 0})")

    def close(self) -> None:
        self.conn.close()
