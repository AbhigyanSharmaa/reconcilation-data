"""Local Spark + Delta Lake connector: the Databricks stand-in for the Docker lab.

Databricks runs Spark SQL on Delta tables, and so does this connector. It therefore
inherits DatabricksConnector's SQL unchanged (backtick quoting, VERSION AS OF time
travel, overflow-safe sums). Only the connection differs: a local SparkSession
instead of a Databricks SQL warehouse.

Needs pyspark and delta-spark, which the lab's Docker image provides
(arf-local-lab/docker/legacy/Dockerfile). It is not used at the client.

settings.yaml example (arf-local-lab/arf-config/settings.yaml):
    legacy_databricks:
      type: spark
      delta_root: /lab/data/delta     # <root>/<database>/<table>/ is registered as database.table
      session_timezone: UTC
      spark_conf:
        spark.driver.memory: 1g
"""

from __future__ import annotations

import logging
from pathlib import Path

from ..config import Endpoint
from ..models import ColumnInfo
from .base import Connector, TableNotFoundError
from .databricks import DatabricksConnector, _split_name

log = logging.getLogger("arf")

DELTA_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}


def build_spark_session(cfg: dict):
    """A SparkSession with Delta Lake enabled.

    The lab notebooks call this too (through lab_common.py), so the notebooks and ARF
    always build Spark the same way and see the same tables.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise ImportError(
            "pyspark is not installed. The 'spark' connection type is meant for the local "
            "Docker lab (arf-local-lab), whose image includes pyspark and delta-spark."
        ) from exc

    use_delta = cfg.get("delta", True)
    builder = (SparkSession.builder
               .appName(cfg.get("app_name", "arf"))
               .master(cfg.get("master", "local[*]"))
               .config("spark.sql.session.timeZone", cfg.get("session_timezone", "UTC")))
    if use_delta:
        for key, value in DELTA_CONF.items():
            builder = builder.config(key, value)
    for key, value in (cfg.get("spark_conf") or {}).items():
        builder = builder.config(key, str(value))
    if use_delta:
        from delta import configure_spark_with_delta_pip   # adds the matching Delta jars
        builder = configure_spark_with_delta_pip(builder)

    spark = builder.getOrCreate()
    if cfg.get("delta_root"):
        registered = register_delta_tables(spark, cfg["delta_root"])
        log.info("Registered %d Delta table(s) from %s", len(registered), cfg["delta_root"])
    return spark


def register_delta_tables(spark, root) -> list[str]:
    """Expose Delta folders laid out as <root>/<database>/<table>/ as database.table.

    Local Spark keeps its table catalog in memory, so every new session (each notebook
    kernel, each ARF run) registers the tables again. The data itself is never touched.
    """
    root = Path(root)
    names: list[str] = []
    if not root.is_dir():
        return names
    for db_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        for table_dir in sorted(p for p in db_dir.iterdir() if (p / "_delta_log").is_dir()):
            spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db_dir.name}`")
            spark.sql(f"CREATE TABLE IF NOT EXISTS `{db_dir.name}`.`{table_dir.name}` "
                      f"USING DELTA LOCATION '{table_dir.resolve().as_posix()}'")
            names.append(f"{db_dir.name}.{table_dir.name}")
    return names


class SparkConnector(DatabricksConnector):
    type_dialect = "databricks"   # Spark reports the same type names as Databricks

    def __init__(self, name: str, cfg: dict):
        Connector.__init__(self, name, cfg)   # skip DatabricksConnector.__init__: no warehouse to reach
        self.spark = build_spark_session(cfg)

    def _execute(self, sql: str) -> list[dict]:
        return [row.asDict() for row in self.spark.sql(sql).collect()]

    def get_schema(self, endpoint: Endpoint) -> list[ColumnInfo]:
        name = ".".join(_split_name(endpoint.table))
        try:
            schema = self.spark.table(name).schema
        except Exception as exc:
            text = str(exc)
            if "TABLE_OR_VIEW_NOT_FOUND" in text or "not found" in text.lower():
                raise TableNotFoundError(
                    f"Table '{endpoint.table}' not found in local Spark "
                    f"(is its folder under delta_root: {self.cfg.get('delta_root')}?)") from exc
            raise
        # simpleString() gives Spark SQL type names such as bigint, decimal(18,2), timestamp.
        return [self.column(f.name, f.dataType.simpleString(), f.nullable, i)
                for i, f in enumerate(schema.fields, start=1)]

    def close(self) -> None:
        if self.cfg.get("stop_on_close", True):
            self.spark.stop()
