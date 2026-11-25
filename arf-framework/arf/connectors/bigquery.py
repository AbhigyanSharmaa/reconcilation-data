"""BigQuery (target side) connector using google-cloud-bigquery.

Authentication uses Application Default Credentials: on your laptop run
`gcloud auth application-default login`; on Composer/Dataproc the service
account is picked up automatically. No key files needed.

settings.yaml example:
    bigquery_target:
      type: bigquery
      project: my-gcp-project          # project that runs (and pays for) the queries
      location: US
      maximum_bytes_billed: 50000000000   # safety cap per query (50 GB); query fails instead of overspending

Local BigQuery emulator (Docker lab only; leave these out for real BigQuery):
      api_endpoint: http://bigquery:9050  # send requests to the emulator instead of Google
      anonymous_credentials: true         # the emulator needs no login
      sum_cast: BIGNUMERIC                # type used inside SUM(); NUMERIC or none if an emulator lacks BIGNUMERIC
"""

from __future__ import annotations

from ..config import Endpoint
from ..hashing import HashColumn
from ..models import RESULT_COLUMNS, ColumnInfo
from ..type_mapping import BOOLEAN, DATE, DATETIME, DECIMAL, FLOAT, INTEGER, STRING, TIMESTAMP
from .base import Connector, TableNotFoundError


class BigQueryConnector(Connector):
    type_dialect = "bigquery"

    def __init__(self, name: str, cfg: dict):
        super().__init__(name, cfg)
        try:
            from google.cloud import bigquery
        except ImportError as exc:
            raise ImportError("Run: pip install google-cloud-bigquery") from exc
        self.bq = bigquery
        extra = {}
        if cfg.get("api_endpoint"):   # local emulator
            from google.api_core.client_options import ClientOptions
            extra["client_options"] = ClientOptions(api_endpoint=cfg["api_endpoint"])
        if cfg.get("anonymous_credentials"):
            from google.auth.credentials import AnonymousCredentials
            extra["credentials"] = AnonymousCredentials()
        self.client = bigquery.Client(project=cfg.get("project"), location=cfg.get("location"), **extra)
        self.max_bytes = cfg.get("maximum_bytes_billed")

    def quote(self, identifier: str) -> str:
        return "`" + identifier.replace("`", "\\`") + "`"

    def table_ref(self, endpoint: Endpoint) -> str:
        if endpoint.as_of_version is not None:
            raise ValueError("as_of_version is Delta-only; use as_of_timestamp for BigQuery time travel")
        ref = f"`{endpoint.table.strip('`')}`"
        if endpoint.as_of_timestamp:
            ref += f" FOR SYSTEM_TIME AS OF TIMESTAMP '{endpoint.as_of_timestamp}'"
        return ref

    def sum_expr(self, quoted_column: str, family: str) -> str:
        # SUM(INT64) and SUM(NUMERIC) raise an overflow error on huge totals; BIGNUMERIC does not.
        cast = str(self.cfg.get("sum_cast", "BIGNUMERIC")).upper()
        if family in (INTEGER, DECIMAL) and cast not in ("NONE", ""):
            return f"SUM(CAST({quoted_column} AS {cast}))"
        return f"SUM({quoted_column})"

    # ---- row hash: GoogleSQL versions of the canonical rules in arf/hashing.py ----
    def literal(self, text: str) -> str:
        return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"

    def hash_md5(self, expr: str) -> str:
        return f"TO_HEX(MD5({expr}))"

    def hash_hex_to_int(self, hex8_expr: str) -> str:
        return f"CAST(CONCAT('0x', {hex8_expr}) AS INT64)"

    def hash_separator(self) -> str:
        return "CODE_POINTS_TO_STRING([31])"

    def hash_canonical(self, quoted_column: str, col: HashColumn) -> str:
        f, q = col.family, quoted_column
        if f == STRING:
            return f"TRIM({q})" if col.trim else q
        if f in (INTEGER, BOOLEAN):
            return f"CAST({q} AS STRING)"
        if f == DECIMAL:
            return f"FORMAT('%.{col.scale}f', {q})"
        if f == FLOAT:
            return f"FORMAT('%.{col.float_digits}f', {q})"
        if f == DATE:
            return f"FORMAT_DATE('%Y-%m-%d', {q})"
        if f == TIMESTAMP:
            return f"FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S', {q}, 'UTC')"
        if f == DATETIME:
            return f"FORMAT_DATETIME('%Y-%m-%d %H:%M:%E6S', {q})"
        raise ValueError(f"{f} columns cannot be hashed")

    def _execute(self, sql: str) -> list[dict]:
        job_config = self.bq.QueryJobConfig(labels={"tool": "arf"})
        if self.max_bytes:
            job_config.maximum_bytes_billed = int(self.max_bytes)
        job = self.client.query(sql, job_config=job_config)
        return [dict(row.items()) for row in job.result()]

    def get_schema(self, endpoint: Endpoint) -> list[ColumnInfo]:
        """Reads table metadata (free, no query cost)."""
        try:
            table = self.client.get_table(endpoint.table.strip("`"))
        except Exception as exc:
            if type(exc).__name__ == "NotFound":   # google.api_core.exceptions.NotFound
                raise TableNotFoundError(f"Table '{endpoint.table}' not found in BigQuery") from exc
            raise
        columns = []
        for i, f in enumerate(table.schema, start=1):
            raw = f.field_type
            if getattr(f, "precision", None) is not None:
                raw += f"({f.precision},{f.scale or 0})"
            if f.mode == "REPEATED":
                raw = f"ARRAY<{raw}>"
            columns.append(self.column(f.name, raw, nullable=f.mode != "REQUIRED", ordinal=i))
        return columns

    def load_rows(self, table: str, rows: list[dict]) -> None:
        """Append result rows with a load job (free, and works in the BigQuery sandbox)."""
        if not rows:
            return
        schema = [self.bq.SchemaField(c, "TIMESTAMP" if c == "run_ts" else "STRING") for c in RESULT_COLUMNS]
        job_config = self.bq.LoadJobConfig(write_disposition="WRITE_APPEND", schema=schema)
        self.client.load_table_from_json(rows, table, job_config=job_config).result()

    def close(self) -> None:
        self.client.close()
