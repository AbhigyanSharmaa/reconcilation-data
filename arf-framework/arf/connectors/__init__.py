"""Connector factory. Each connection in settings.yaml is opened once per run and reused."""

from __future__ import annotations

from ..hashing import HashColumn
from .base import Connector, MetricSpec, TableNotFoundError

__all__ = ["Connector", "ConnectorPool", "HashColumn", "MetricSpec", "TableNotFoundError", "create_connector"]


def create_connector(name: str, cfg: dict) -> Connector:
    kind = cfg.get("type")
    if kind == "sqlite":
        from .sqlite import SQLiteConnector
        return SQLiteConnector(name, cfg)
    if kind == "databricks":
        from .databricks import DatabricksConnector
        return DatabricksConnector(name, cfg)
    if kind == "bigquery":
        from .bigquery import BigQueryConnector
        return BigQueryConnector(name, cfg)
    if kind == "spark":   # local Spark + Delta, used by the Docker lab
        from .spark import SparkConnector
        return SparkConnector(name, cfg)
    raise ValueError(f"Connection '{name}': unknown type '{kind}'")


class ConnectorPool:
    def __init__(self, connections: dict):
        self.connections = connections
        self._open: dict[str, Connector] = {}
        self._failed: dict[str, Exception] = {}

    def get(self, name: str) -> Connector:
        # A connection that failed once (wrong token, no network) is not retried for every table.
        if name in self._failed:
            raise self._failed[name]
        if name not in self._open:
            try:
                self._open[name] = create_connector(name, self.connections[name])
            except Exception as exc:
                self._failed[name] = exc
                raise
        return self._open[name]

    def close_all(self) -> None:
        for conn in self._open.values():
            try:
                conn.close()
            except Exception:
                pass
        self._open.clear()
