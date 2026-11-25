"""Shared helpers for the lab notebooks.

Everything here comes from the ARF project mounted at /arf, so the notebooks and ARF
share one definition of the Spark session, the BigQuery connection and the sample data:
  * Spark settings and BigQuery settings  <- /lab/arf-config/settings.yaml (read by arf.config)
  * SparkSession with Delta Lake          <- arf.connectors.spark.build_spark_session
  * BigQuery client for the emulator      <- arf.connectors.bigquery.BigQueryConnector
"""

from pathlib import Path

from arf.config import load_settings
from arf.connectors import create_connector
from arf.connectors.spark import build_spark_session

LAB = Path(__file__).resolve().parents[1]                 # /lab inside the container
SETTINGS = load_settings(LAB / "arf-config" / "settings.yaml")
SPARK_CFG = SETTINGS.connections["legacy_databricks"]
BQ_CFG = SETTINGS.connections["bigquery_target"]

DELTA_ROOT = Path(SPARK_CFG["delta_root"])                # /lab/data/delta
BQ_PROJECT = BQ_CFG["project"]                            # local-project
BQ_DATASET = "sales"


def get_spark():
    """The same SparkSession ARF builds; every Delta table under DELTA_ROOT is registered."""
    return build_spark_session({**SPARK_CFG, "app_name": "lab-notebook"})


def bq_client():
    """A google-cloud-bigquery client pointed at the emulator, built by ARF's own connector."""
    return create_connector("bigquery_target", BQ_CFG).client


def delta_path(database: str, table: str) -> str:
    return (DELTA_ROOT / database / table).as_posix()


def bq_table(table: str) -> str:
    return f"{BQ_PROJECT}.{BQ_DATASET}.{table}"
