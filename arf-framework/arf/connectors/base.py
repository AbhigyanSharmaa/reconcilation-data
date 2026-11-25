"""Common interface every engine connector implements.

The comparison modules only ever call get_schema(), row_count() and profile().
They never know whether they are talking to Databricks, BigQuery or a local
SQLite test file - so adding a new engine means adding one connector class.
"""

from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass

from ..config import Endpoint
from ..hashing import NULL_TOKEN, HashColumn
from ..models import ColumnInfo
from ..type_mapping import parse_type

log = logging.getLogger("arf")


class TableNotFoundError(Exception):
    pass


@dataclass
class MetricSpec:
    """One aggregate to compute on one side, e.g. SUM(amount) aliased m3."""

    alias: str
    metric: str   # sum | min | max | null_count
    column: str   # column name as it exists on THIS side
    family: str   # type family on THIS side


class Connector(ABC):
    type_dialect = "databricks"   # which type vocabulary get_schema() returns

    def __init__(self, name: str, cfg: dict):
        self.name = name
        self.cfg = cfg

    # ---- each engine implements these ----------------------------------
    @abstractmethod
    def get_schema(self, endpoint: Endpoint) -> list[ColumnInfo]:
        """Columns of the table in ordinal order. Raise TableNotFoundError if it does not exist."""

    @abstractmethod
    def _execute(self, sql: str) -> list[dict]:
        """Run SQL and return rows as dicts."""

    @abstractmethod
    def quote(self, identifier: str) -> str:
        """Quote a column or table name for this engine."""

    @abstractmethod
    def table_ref(self, endpoint: Endpoint) -> str:
        """Fully quoted table reference, including any time-travel clause."""

    def close(self) -> None:
        pass

    # ---- shared logic -----------------------------------------------------
    def query(self, sql: str) -> list[dict]:
        log.debug("[%s] SQL: %s", self.name, sql)
        return self._execute(sql)

    def sum_expr(self, quoted_column: str, family: str) -> str:
        return f"SUM({quoted_column})"

    def where(self, endpoint: Endpoint) -> str:
        return f" WHERE ({endpoint.filter})" if endpoint.filter else ""

    def column(self, name: str, raw_type: str, nullable: bool | None, ordinal: int | None) -> ColumnInfo:
        family, precision, scale = parse_type(raw_type, self.type_dialect)
        return ColumnInfo(name=name, raw_type=raw_type, family=family, nullable=nullable,
                          precision=precision, scale=scale, ordinal=ordinal)

    def row_count(self, endpoint: Endpoint) -> int:
        sql = f"SELECT COUNT(*) AS row_count FROM {self.table_ref(endpoint)}{self.where(endpoint)}"
        rows = self.query(sql)
        return int(next(iter(rows[0].values())))

    def metric_expr(self, spec: MetricSpec) -> str:
        col = self.quote(spec.column)
        if spec.metric == "sum":
            return self.sum_expr(col, spec.family)
        if spec.metric == "min":
            return f"MIN({col})"
        if spec.metric == "max":
            return f"MAX({col})"
        if spec.metric == "null_count":
            return f"COUNT(*) - COUNT({col})"
        raise ValueError(f"Unknown metric '{spec.metric}'")

    def profile_sql(self, endpoint: Endpoint, specs: list[MetricSpec]) -> str:
        exprs = ",\n  ".join(f"{self.metric_expr(s)} AS {s.alias}" for s in specs)
        return f"SELECT\n  {exprs}\nFROM {self.table_ref(endpoint)}{self.where(endpoint)}"

    def profile(self, endpoint: Endpoint, specs: list[MetricSpec], chunk_size: int = 300) -> dict:
        """All aggregates in as few full-table scans as possible (chunked for very wide tables)."""
        values: dict = {}
        for start in range(0, len(specs), chunk_size):
            chunk = specs[start:start + chunk_size]
            rows = self.query(self.profile_sql(endpoint, chunk))
            row = {k.lower(): v for k, v in rows[0].items()}
            for spec in chunk:
                values[spec.alias] = row.get(spec.alias.lower())
        return values

    # ---- row hash validation ------------------------------------------------
    # Each engine supplies these small SQL pieces; the queries themselves are shared below.
    def literal(self, text: str) -> str:
        """A SQL string literal."""
        raise NotImplementedError(f"{type(self).__name__} does not support row hashing")

    def hash_md5(self, expr: str) -> str:
        """MD5 of a text expression, as 32 lowercase hex characters."""
        raise NotImplementedError(f"{type(self).__name__} does not support row hashing")

    def hash_hex_to_int(self, hex8_expr: str) -> str:
        """An 8-character hex string as a whole number."""
        raise NotImplementedError(f"{type(self).__name__} does not support row hashing")

    def hash_mod(self, expr: str, n: int) -> str:
        return f"MOD({expr}, {n})"

    def hash_concat(self, parts: list[str]) -> str:
        return f"CONCAT({', '.join(parts)})"

    def hash_separator(self) -> str:
        raise NotImplementedError(f"{type(self).__name__} does not support row hashing")

    def hash_canonical(self, quoted_column: str, col: HashColumn) -> str:
        """The column's value as canonical text (rules in arf/hashing.py). NULLs are handled by the caller."""
        raise NotImplementedError(f"{type(self).__name__} does not support row hashing")

    def _hash_text(self, cols: list[HashColumn]) -> str:
        parts: list[str] = []
        for col in cols:
            if parts:
                parts.append(self.hash_separator())
            q = self.quote(col.name)
            parts.append(f"CASE WHEN {q} IS NULL THEN {self.literal(NULL_TOKEN)} "
                         f"ELSE {self.hash_canonical(q, col)} END")
        return parts[0] if len(parts) == 1 else self.hash_concat(parts)

    def _hash_inner_sql(self, endpoint: Endpoint, keys: list[HashColumn], cols: list[HashColumn],
                        buckets: int) -> str:
        key_text = self._hash_text(keys)
        bucket = self.hash_mod(self.hash_hex_to_int(f"SUBSTR({self.hash_md5(key_text)}, 1, 8)"), buckets)
        return (f"SELECT {key_text} AS arf_key, {self.hash_md5(self._hash_text(cols or keys))} AS arf_row_hash, "
                f"{bucket} AS arf_bucket FROM {self.table_ref(endpoint)}{self.where(endpoint)}")

    def hash_bucket_sql(self, endpoint: Endpoint, keys: list[HashColumn], cols: list[HashColumn],
                        buckets: int) -> str:
        """One row per bucket: row count and two order-independent sums of the row hashes."""
        h1 = self.hash_hex_to_int("SUBSTR(arf_row_hash, 1, 8)")
        h2 = self.hash_hex_to_int("SUBSTR(arf_row_hash, 9, 8)")
        return (f"SELECT arf_bucket, COUNT(*) AS arf_rows, SUM({h1}) AS arf_h1, SUM({h2}) AS arf_h2 "
                f"FROM ({self._hash_inner_sql(endpoint, keys, cols, buckets)}) arf_t GROUP BY arf_bucket")

    def hash_buckets(self, endpoint: Endpoint, keys: list[HashColumn], cols: list[HashColumn],
                     buckets: int) -> dict[int, tuple[int, int, int]]:
        out = {}
        for row in self.query(self.hash_bucket_sql(endpoint, keys, cols, buckets)):
            r = {k.lower(): v for k, v in row.items()}
            out[int(r["arf_bucket"])] = (int(r["arf_rows"]), int(r["arf_h1"] or 0), int(r["arf_h2"] or 0))
        return out

    def hash_rows(self, endpoint: Endpoint, keys: list[HashColumn], cols: list[HashColumn], buckets: int,
                  bucket_ids: list[int]) -> dict[str, str]:
        """Key -> row hash for every row in the given buckets (the drill-down)."""
        ids = ", ".join(str(int(b)) for b in bucket_ids)
        sql = (f"SELECT arf_key, arf_row_hash FROM ({self._hash_inner_sql(endpoint, keys, cols, buckets)}) arf_t "
               f"WHERE arf_bucket IN ({ids})")
        out = {}
        for row in self.query(sql):
            r = {k.lower(): v for k, v in row.items()}
            out[str(r["arf_key"])] = str(r["arf_row_hash"])
        return out

    def hash_values(self, endpoint: Endpoint, keys: list[HashColumn], cols: list[HashColumn],
                    key_texts: list[str]) -> dict[str, list[str]]:
        """Key -> canonical text of each column, for a few changed rows (to name the changed columns)."""
        if not key_texts or not cols:
            return {}
        values = ", ".join(f"{self._hash_text([c])} AS arf_v{i}" for i, c in enumerate(cols))
        wanted = ", ".join(self.literal(k) for k in key_texts)
        sql = (f"SELECT * FROM (SELECT {self._hash_text(keys)} AS arf_key, {values} "
               f"FROM {self.table_ref(endpoint)}{self.where(endpoint)}) arf_t WHERE arf_key IN ({wanted})")
        out = {}
        for row in self.query(sql):
            r = {k.lower(): v for k, v in row.items()}
            out[str(r["arf_key"])] = [str(r[f"arf_v{i}"]) for i in range(len(cols))]
        return out


def resolve_secret(cfg: dict, key: str, connection_name: str) -> str:
    """Read a secret without ever storing it in YAML or Git.

    Order: <key>_env (environment variable) -> <key>_secret (GCP Secret Manager)
    -> <key> (plain text, discouraged; logs a warning).
    """
    env_name = cfg.get(f"{key}_env")
    if env_name:
        value = os.environ.get(env_name)
        if not value:
            raise ValueError(f"Connection '{connection_name}': environment variable {env_name} is not set")
        return value

    secret_name = cfg.get(f"{key}_secret")
    if secret_name:
        try:
            from google.cloud import secretmanager
        except ImportError as exc:
            raise ImportError("Install google-cloud-secret-manager to read secrets from Secret Manager") from exc
        client = secretmanager.SecretManagerServiceClient()
        return client.access_secret_version(name=secret_name).payload.data.decode("utf-8")

    if cfg.get(key):
        log.warning("Connection '%s': %s is stored in plain text in settings.yaml - "
                    "use %s_env or %s_secret instead", connection_name, key, key, key)
        return cfg[key]

    raise ValueError(f"Connection '{connection_name}': set {key}_env or {key}_secret")
