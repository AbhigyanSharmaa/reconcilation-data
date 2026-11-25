"""Databricks (legacy side) connector using databricks-sql-connector.

Runs SQL on a Databricks SQL warehouse or all-purpose cluster. Only aggregate
results (a few rows) come back to Python - full tables never leave Azure.

settings.yaml example:
    databricks_legacy:
      type: databricks
      server_hostname: adb-1234567890123456.7.azuredatabricks.net
      http_path: /sql/1.0/warehouses/abcdef1234567890
      access_token_env: DATABRICKS_TOKEN      # or access_token_secret: projects/<p>/secrets/<s>/versions/latest
      session_timezone: UTC                   # timestamps are read in UTC, same as BigQuery
"""

from __future__ import annotations

from ..config import Endpoint
from ..hashing import HashColumn
from ..models import ColumnInfo
from ..type_mapping import BOOLEAN, DATE, DATETIME, DECIMAL, FLOAT, INTEGER, STRING, TIMESTAMP
from .base import Connector, TableNotFoundError, resolve_secret


def _split_name(table: str) -> list[str]:
    """'catalog.schema.table' or `catalog`.`schema`.`table` -> ['catalog', 'schema', 'table']."""
    return [part.strip().strip("`") for part in table.split(".")]


class DatabricksConnector(Connector):
    type_dialect = "databricks"

    def __init__(self, name: str, cfg: dict):
        super().__init__(name, cfg)
        try:
            from databricks import sql as dbsql
        except ImportError as exc:
            raise ImportError("Run: pip install databricks-sql-connector") from exc
        for key in ("server_hostname", "http_path"):
            if not cfg.get(key):
                raise ValueError(f"Connection '{name}': '{key}' is required for databricks")
        self.conn = dbsql.connect(
            server_hostname=cfg["server_hostname"],
            http_path=cfg["http_path"],
            access_token=resolve_secret(cfg, "access_token", name),
        )
        timezone = cfg.get("session_timezone", "UTC")
        if timezone:
            self.query(f"SET TIME ZONE '{timezone}'")

    def quote(self, identifier: str) -> str:
        return "`" + identifier.replace("`", "``") + "`"

    def table_ref(self, endpoint: Endpoint) -> str:
        ref = ".".join(self.quote(p) for p in _split_name(endpoint.table))
        if endpoint.as_of_version is not None:
            ref += f" VERSION AS OF {int(endpoint.as_of_version)}"
        elif endpoint.as_of_timestamp:
            ref += f" TIMESTAMP AS OF '{endpoint.as_of_timestamp}'"
        return ref

    def sum_expr(self, quoted_column: str, family: str) -> str:
        # BIGINT sums can overflow; DECIMAL(38,0) cannot for any realistic table.
        if family == INTEGER:
            return f"SUM(CAST({quoted_column} AS DECIMAL(38,0)))"
        return f"SUM({quoted_column})"

    # ---- row hash: Spark SQL versions of the canonical rules in arf/hashing.py ----
    def literal(self, text: str) -> str:
        return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"

    def hash_md5(self, expr: str) -> str:
        return f"md5({expr})"

    def hash_hex_to_int(self, hex8_expr: str) -> str:
        return f"CAST(conv({hex8_expr}, 16, 10) AS BIGINT)"

    def hash_mod(self, expr: str, n: int) -> str:
        return f"pmod({expr}, {n})"

    def hash_concat(self, parts: list[str]) -> str:
        return f"concat({', '.join(parts)})"

    def hash_separator(self) -> str:
        return "chr(31)"

    def hash_canonical(self, quoted_column: str, col: HashColumn) -> str:
        f, q = col.family, quoted_column
        if f == STRING:
            return f"trim({q})" if col.trim else q
        if f in (INTEGER, BOOLEAN):
            return f"CAST({q} AS STRING)"
        if f == DECIMAL:
            return f"format_string('%.{col.scale}f', {q})"
        if f == FLOAT:
            return f"format_string('%.{col.float_digits}f', {q})"
        if f == DATE:
            return f"date_format({q}, 'yyyy-MM-dd')"
        if f in (TIMESTAMP, DATETIME):   # session time zone is UTC (SET TIME ZONE at connect)
            return f"date_format({q}, 'yyyy-MM-dd HH:mm:ss.SSSSSS')"
        raise ValueError(f"{f} columns cannot be hashed")

    def _execute(self, sql: str) -> list[dict]:
        with self.conn.cursor() as cur:
            cur.execute(sql)
            if cur.description is None:   # statements such as SET return no rows
                return []
            columns = [d[0] for d in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_schema(self, endpoint: Endpoint) -> list[ColumnInfo]:
        parts = _split_name(endpoint.table)
        if len(parts) == 3:   # catalog.schema.table -> metadata call, which includes nullability
            columns = self._schema_from_metadata(parts)
            if columns:
                return columns
        # schema.table resolves against the session's current catalog, which DESCRIBE respects.
        return self._schema_from_describe(endpoint)

    def _schema_from_metadata(self, parts: list[str]) -> list[ColumnInfo]:
        """cursor.columns() works for Unity Catalog and hive_metastore and returns nullability."""
        catalog, schema, table = parts
        with self.conn.cursor() as cur:
            cur.columns(catalog_name=catalog, schema_name=schema, table_name=table)
            names = [d[0] for d in cur.description]
            rows = [dict(zip(names, r)) for r in cur.fetchall()]
        # Name arguments are LIKE patterns ('_' matches any character), so keep exact matches only.
        rows = [r for r in rows
                if str(r.get("TABLE_CAT", "")).lower() == catalog.lower()
                and str(r.get("TABLE_SCHEM", "")).lower() == schema.lower()
                and str(r.get("TABLE_NAME", "")).lower() == table.lower()]
        rows.sort(key=lambda r: r.get("ORDINAL_POSITION") or 0)

        columns = []
        for i, r in enumerate(rows, start=1):
            raw = str(r.get("TYPE_NAME") or "")
            size, digits = r.get("COLUMN_SIZE"), r.get("DECIMAL_DIGITS")
            if raw.upper().startswith("DECIMAL") and "(" not in raw and size is not None:
                raw = f"DECIMAL({size},{digits or 0})"
            if r.get("IS_NULLABLE") in ("YES", "NO"):
                nullable = r["IS_NULLABLE"] == "YES"
            elif r.get("NULLABLE") in (0, 1):
                nullable = r["NULLABLE"] == 1
            else:
                nullable = None
            columns.append(self.column(r["COLUMN_NAME"], raw, nullable, r.get("ORDINAL_POSITION") or i))
        return columns

    def _schema_from_describe(self, endpoint: Endpoint) -> list[ColumnInfo]:
        """Fallback: DESCRIBE TABLE gives names and types but not nullability."""
        try:
            rows = self.query(f"DESCRIBE TABLE {self.table_ref(Endpoint(endpoint.connection, endpoint.table))}")
        except Exception as exc:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(exc) or "not found" in str(exc).lower():
                raise TableNotFoundError(f"Table '{endpoint.table}' not found on {self.name}") from exc
            raise
        columns = []
        for i, r in enumerate(rows, start=1):
            name = (r.get("col_name") or "").strip()
            if not name or name.startswith("#"):   # partition / detail sections start here
                break
            columns.append(self.column(name, r.get("data_type") or "", None, i))
        if not columns:
            raise TableNotFoundError(f"Table '{endpoint.table}' returned no columns on {self.name}")
        return columns

    def close(self) -> None:
        self.conn.close()
