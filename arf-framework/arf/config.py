"""Load and validate settings.yaml and the per-table YAML files.

Unknown keys are rejected on purpose: a typo such as 'fliter' would otherwise be
silently ignored and the comparison would run on the wrong data.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, fields, replace
from pathlib import Path

import yaml

from .tolerance import DEFAULT_TIMESTAMP_FORMATS

VALID_CHECKS = ["schema", "row_count", "numeric", "row_hash"]
DEFAULT_CHECKS = ["schema", "row_count", "numeric"]   # row_hash is opt-in per table: it scans every column
VALID_SINKS = ["console", "csv", "bigquery"]
VALID_SEVERITIES = ["critical", "warning"]

DEFAULT_SEVERITY = {
    "missing_column": "critical",    # column in Databricks but not in BigQuery
    "extra_column": "warning",       # column in BigQuery but not in Databricks
    "data_type": "critical",         # type families differ (e.g. INT -> STRING)
    "nullability": "warning",        # NOT NULL on one side only
    "decimal_precision": "warning",  # decimal scale or integer digits reduced
    "row_count": "critical",
    "numeric": "critical",           # SUM / MIN / MAX
    "null_count": "critical",
    "row_hash": "critical",          # rows missing, extra or changed (row hash validation)
}


class ConfigError(Exception):
    """Raised with a message that says which file and which key is wrong."""


@dataclass
class Tolerance:
    row_count_abs: float = 0
    row_count_pct: float = 0.0
    null_count_abs: float = 0
    null_count_pct: float = 0.0
    numeric_abs: float = 1e-6
    numeric_rel: float = 1e-9
    timestamp_seconds: float = 0
    source_timezone: str = "UTC"
    target_timezone: str = "UTC"
    timestamp_formats: list = field(default_factory=lambda: list(DEFAULT_TIMESTAMP_FORMATS))

    def merged(self, overrides: dict | None, where: str) -> "Tolerance":
        if not overrides:
            return self
        if not isinstance(overrides, dict):
            raise ConfigError(f"{where}: tolerance must be a mapping of key: value")
        allowed = {f.name for f in fields(self)}
        _check_keys(overrides, allowed, where)
        clean = dict(overrides)
        for key, value in overrides.items():
            if key in ("source_timezone", "target_timezone"):
                clean[key] = str(value)
            elif key == "timestamp_formats":
                if not isinstance(value, list):
                    raise ConfigError(f"{where}.timestamp_formats must be a list of strftime formats")
            else:
                try:   # YAML reads 1e-6 (no dot) as text, so convert explicitly
                    clean[key] = float(value)
                except (TypeError, ValueError):
                    raise ConfigError(f"{where}.{key} must be a number, got '{value}'") from None
                if clean[key] < 0:
                    raise ConfigError(f"{where}.{key} cannot be negative")
        return replace(self, **clean)

    def describe_counts(self, kind: str) -> str:
        abs_tol = getattr(self, f"{kind}_abs")
        pct_tol = getattr(self, f"{kind}_pct")
        return f"abs<={abs_tol:g}, pct<={pct_tol:g}%" if (abs_tol or pct_tol) else "exact"

    def describe_numeric(self) -> str:
        return f"abs<={self.numeric_abs:g}, rel<={self.numeric_rel:g}"

    def describe_temporal(self) -> str:
        return f"<={self.timestamp_seconds:g}s ({self.source_timezone} vs {self.target_timezone})"


@dataclass
class Endpoint:
    connection: str
    table: str
    filter: str | None = None
    as_of_version: int | None = None      # Delta time travel (Databricks only)
    as_of_timestamp: str | None = None    # Delta / BigQuery time travel


@dataclass
class HashConfig:
    """Settings for the row_hash check (table YAML key: hash)."""
    columns: object = "auto"         # "auto" = every comparable column, or a list of target names
    exclude_columns: list = field(default_factory=list)
    buckets: int = 256               # rows are grouped into this many buckets by primary key
    max_buckets_to_inspect: int = 32 # differing buckets drilled into, row by row
    max_examples: int = 20           # example rows logged per kind (missing / extra / changed)
    decimal_scale: object = "auto"   # "auto" = the smaller scale of the two sides
    float_digits: int = 6            # FLOAT/DOUBLE values are rounded to this many decimals
    trim_strings: bool = False       # ignore leading/trailing spaces in text


@dataclass
class OutputConfig:
    sinks: list = field(default_factory=lambda: ["console", "csv"])
    output_dir: str = "output"
    bigquery_connection: str | None = None
    results_table: str | None = None
    exceptions_table: str | None = None


@dataclass
class Settings:
    connections: dict
    output: OutputConfig
    default_checks: list
    default_tolerance: Tolerance
    default_severity: dict
    allowed_type_changes: list
    max_metrics_per_query: int = 300
    path: str | None = None


@dataclass
class TableConfig:
    table_id: str
    source: Endpoint
    target: Endpoint
    checks: list
    column_mapping: dict          # source column -> target column
    ignore_columns: list
    numeric_columns: object       # "auto" or list of target column names
    null_count_columns: object    # "all", "profiled" or list of target column names
    case_sensitive: bool
    tolerance: Tolerance
    column_tolerance: dict        # target column -> Tolerance
    severity: dict
    allowed_type_changes: list
    primary_key: list = field(default_factory=list)   # source column names
    hash: HashConfig = field(default_factory=HashConfig)
    enabled: bool = True
    path: str | None = None

    def key(self, name: str) -> str:
        """Column names are compared case-insensitively unless case_sensitive: true."""
        return name if self.case_sensitive else name.lower()

    def tolerance_for(self, target_column: str) -> Tolerance:
        return self.column_tolerance.get(self.key(target_column), self.tolerance)


# ---------------------------------------------------------------- helpers

def _check_keys(data: dict, allowed: set, where: str) -> None:
    unknown = sorted(set(data) - set(allowed))
    if unknown:
        raise ConfigError(f"{where}: unknown key(s) {unknown}. Allowed: {sorted(allowed)}")


def _require(data: dict, key: str, where: str):
    value = data.get(key)
    if value in (None, ""):
        raise ConfigError(f"{where}: '{key}' is required")
    return value


def _expand_env(value):
    """Allow ${GCP_PROJECT}-style environment variables anywhere in the YAML."""
    if isinstance(value, str):
        return os.path.expandvars(value)
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    return value


def _read_yaml(path: Path) -> dict:
    if not path.exists():
        raise ConfigError(f"File not found: {path}")
    try:
        with path.open(encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path}: invalid YAML - {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: top level must be a mapping (key: value)")
    return _expand_env(data)


def _validate_severity(sev: dict, where: str) -> dict:
    _check_keys(sev, set(DEFAULT_SEVERITY), where)
    for key, value in sev.items():
        if value not in VALID_SEVERITIES:
            raise ConfigError(f"{where}.{key}: must be one of {VALID_SEVERITIES}, got '{value}'")
    return sev


def _validate_checks(checks, where: str) -> list:
    if not isinstance(checks, list) or not checks:
        raise ConfigError(f"{where}: must be a non-empty list, e.g. [schema, row_count, numeric]")
    bad = [c for c in checks if c not in VALID_CHECKS]
    if bad:
        raise ConfigError(f"{where}: unknown check(s) {bad}. Allowed: {VALID_CHECKS}")
    return list(checks)


# --------------------------------------------------------------- settings

def load_settings(path: str | Path) -> Settings:
    path = Path(path)
    data = _read_yaml(path)
    where = str(path)
    _check_keys(data, {"connections", "output", "defaults"}, where)

    connections = _require(data, "connections", where)
    if not isinstance(connections, dict):
        raise ConfigError(f"{where}: 'connections' must be a mapping of name -> settings")
    for name, conn in connections.items():
        if not isinstance(conn, dict) or "type" not in conn:
            raise ConfigError(f"{where}: connection '{name}' needs a 'type' "
                              "(databricks, bigquery, spark or sqlite)")
        if conn["type"] not in ("databricks", "bigquery", "spark", "sqlite"):
            raise ConfigError(f"{where}: connection '{name}' has unknown type '{conn['type']}'")

    out = data.get("output") or {}
    _check_keys(out, {"sinks", "output_dir", "bigquery"}, f"{where}: output")
    sinks = out.get("sinks", ["console", "csv"])
    bad = [s for s in sinks if s not in VALID_SINKS]
    if bad:
        raise ConfigError(f"{where}: output.sinks has unknown value(s) {bad}. Allowed: {VALID_SINKS}")
    bq = out.get("bigquery") or {}
    _check_keys(bq, {"connection", "results_table", "exceptions_table"}, f"{where}: output.bigquery")
    output = OutputConfig(
        sinks=list(sinks),
        output_dir=out.get("output_dir", "output"),
        bigquery_connection=bq.get("connection"),
        results_table=bq.get("results_table"),
        exceptions_table=bq.get("exceptions_table"),
    )
    if "bigquery" in output.sinks:
        if not (output.bigquery_connection and output.results_table and output.exceptions_table):
            raise ConfigError(f"{where}: sink 'bigquery' needs output.bigquery.connection, "
                              "results_table and exceptions_table")
        if output.bigquery_connection not in connections:
            raise ConfigError(f"{where}: output.bigquery.connection '{output.bigquery_connection}' "
                              "is not defined under connections")

    defaults = data.get("defaults") or {}
    _check_keys(defaults, {"checks", "tolerance", "severity", "allowed_type_changes", "max_metrics_per_query"},
                f"{where}: defaults")
    tolerance = Tolerance().merged(defaults.get("tolerance"), f"{where}: defaults.tolerance")
    severity = {**DEFAULT_SEVERITY,
                **_validate_severity(defaults.get("severity") or {}, f"{where}: defaults.severity")}
    checks = _validate_checks(defaults.get("checks", DEFAULT_CHECKS), f"{where}: defaults.checks")

    return Settings(
        connections=connections,
        output=output,
        default_checks=checks,
        default_tolerance=tolerance,
        default_severity=severity,
        allowed_type_changes=defaults.get("allowed_type_changes") or [],
        max_metrics_per_query=int(defaults.get("max_metrics_per_query", 300)),
        path=str(path),
    )


# ----------------------------------------------------------------- tables

_TABLE_KEYS = {
    "table_id", "enabled", "source", "target", "checks", "column_mapping", "ignore_columns",
    "numeric_columns", "null_count_columns", "case_sensitive", "tolerance", "column_tolerance",
    "severity", "allowed_type_changes", "primary_key", "hash",
}
_ENDPOINT_KEYS = {"connection", "table", "filter", "as_of_version", "as_of_timestamp"}


def _endpoint(data, settings: Settings, where: str) -> Endpoint:
    if not isinstance(data, dict):
        raise ConfigError(f"{where}: must be a mapping with connection and table")
    _check_keys(data, _ENDPOINT_KEYS, where)
    conn = _require(data, "connection", where)
    if conn not in settings.connections:
        raise ConfigError(f"{where}: connection '{conn}' is not defined in {settings.path}")
    return Endpoint(
        connection=conn,
        table=_require(data, "table", where),
        filter=data.get("filter"),
        as_of_version=data.get("as_of_version"),
        as_of_timestamp=data.get("as_of_timestamp"),
    )


def _parse_hash(data, norm, where: str) -> HashConfig:
    if not data:
        return HashConfig()
    if not isinstance(data, dict):
        raise ConfigError(f"{where}: must be a mapping of key: value")
    _check_keys(data, {f.name for f in fields(HashConfig)}, where)
    cfg = HashConfig(**data)
    if cfg.columns != "auto":
        if not isinstance(cfg.columns, list):
            raise ConfigError(f"{where}.columns must be 'auto' or a list of target column names")
        cfg.columns = [norm(c) for c in cfg.columns]
    if not isinstance(cfg.exclude_columns, list):
        raise ConfigError(f"{where}.exclude_columns must be a list")
    cfg.exclude_columns = [norm(c) for c in cfg.exclude_columns]
    for key in ("buckets", "max_buckets_to_inspect", "max_examples", "float_digits"):
        value = getattr(cfg, key)
        if not isinstance(value, int) or isinstance(value, bool) or value < (0 if key == "float_digits" else 1):
            raise ConfigError(f"{where}.{key} must be a positive whole number, got '{value}'")
    if cfg.decimal_scale != "auto" and (not isinstance(cfg.decimal_scale, int) or cfg.decimal_scale < 0):
        raise ConfigError(f"{where}.decimal_scale must be 'auto' or a whole number >= 0")
    cfg.trim_strings = bool(cfg.trim_strings)
    return cfg


def parse_table(data: dict, settings: Settings, where: str) -> TableConfig:
    _check_keys(data, _TABLE_KEYS, where)
    table_id = _require(data, "table_id", where)
    where = f"{where} [{table_id}]"
    case_sensitive = bool(data.get("case_sensitive", False))
    norm = (lambda s: str(s)) if case_sensitive else (lambda s: str(s).lower())

    mapping = data.get("column_mapping") or {}
    if not isinstance(mapping, dict):
        raise ConfigError(f"{where}: column_mapping must be source_name: target_name pairs")

    numeric_columns = data.get("numeric_columns", "auto")
    if numeric_columns != "auto" and not isinstance(numeric_columns, list):
        raise ConfigError(f"{where}: numeric_columns must be 'auto' or a list of target column names")
    null_cols = data.get("null_count_columns", "all")
    if null_cols not in ("all", "profiled") and not isinstance(null_cols, list):
        raise ConfigError(f"{where}: null_count_columns must be 'all', 'profiled' or a list")

    tolerance = settings.default_tolerance.merged(data.get("tolerance"), f"{where}: tolerance")
    column_tolerance = {}
    for col, overrides in (data.get("column_tolerance") or {}).items():
        column_tolerance[norm(col)] = tolerance.merged(overrides, f"{where}: column_tolerance.{col}")

    checks = _validate_checks(data.get("checks", settings.default_checks), f"{where}: checks")
    primary_key = data.get("primary_key") or []
    if isinstance(primary_key, str):
        primary_key = [primary_key]
    if not isinstance(primary_key, list):
        raise ConfigError(f"{where}: primary_key must be a column name or a list of source column names")
    if "row_hash" in checks and not primary_key:
        raise ConfigError(f"{where}: the row_hash check needs primary_key (the source column(s) that identify a row)")
    hash_cfg = _parse_hash(data.get("hash"), norm, f"{where}: hash")

    return TableConfig(
        table_id=str(table_id),
        source=_endpoint(data.get("source"), settings, f"{where}: source"),
        target=_endpoint(data.get("target"), settings, f"{where}: target"),
        checks=checks,
        column_mapping={norm(k): norm(v) for k, v in mapping.items()},
        ignore_columns=[norm(c) for c in data.get("ignore_columns") or []],
        numeric_columns=numeric_columns if numeric_columns == "auto" else [norm(c) for c in numeric_columns],
        null_count_columns=null_cols if not isinstance(null_cols, list) else [norm(c) for c in null_cols],
        case_sensitive=case_sensitive,
        tolerance=tolerance,
        column_tolerance=column_tolerance,
        severity={**settings.default_severity,
                  **_validate_severity(data.get("severity") or {}, f"{where}: severity")},
        allowed_type_changes=settings.allowed_type_changes + (data.get("allowed_type_changes") or []),
        primary_key=[norm(c) for c in primary_key],
        hash=hash_cfg,
        enabled=bool(data.get("enabled", True)),
        path=where,
    )


def load_tables(path: str | Path, settings: Settings, only: list[str] | None = None) -> list[TableConfig]:
    """Load one YAML file, or every *.yaml / *.yml file in a folder.

    A file holds either one table (table_id: ... at the top) or many (tables: [ ... ]).
    """
    path = Path(path)
    if path.is_dir():
        files = sorted(list(path.glob("*.yaml")) + list(path.glob("*.yml")))
    elif path.exists():
        files = [path]
    else:
        raise ConfigError(f"Table config path not found: {path}")

    tables: list[TableConfig] = []
    for file in files:
        data = _read_yaml(file)
        entries = data["tables"] if "tables" in data else [data]
        if not isinstance(entries, list):
            raise ConfigError(f"{file}: 'tables' must be a list")
        for i, entry in enumerate(entries):
            tables.append(parse_table(entry, settings, f"{file}#{i + 1}"))

    seen = set()
    for t in tables:
        if t.table_id in seen:
            raise ConfigError(f"Duplicate table_id '{t.table_id}' ({t.path})")
        seen.add(t.table_id)

    if only:
        missing = sorted(set(only) - seen)
        if missing:
            raise ConfigError(f"--only names table_id(s) not found in {path}: {missing}")
        tables = [t for t in tables if t.table_id in only]
    return [t for t in tables if t.enabled]
