"""Shared test helpers. Every test runs offline - no Databricks or BigQuery needed."""

from pathlib import Path

import pytest
import yaml

from arf.config import load_settings, parse_table
from arf.models import ColumnInfo
from arf.type_mapping import parse_type


def write_yaml(path: Path, data: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
    return path


@pytest.fixture
def settings_file(tmp_path):
    """settings.yaml with two SQLite connections named like the real ones."""
    return write_yaml(tmp_path / "settings.yaml", {
        "connections": {
            "legacy_databricks": {"type": "sqlite", "path": str(tmp_path / "legacy_databricks.db"),
                                  "emulate": "databricks"},
            "target_bigquery": {"type": "sqlite", "path": str(tmp_path / "target_bigquery.db"),
                                "emulate": "bigquery"},
        },
        "output": {"sinks": ["csv"], "output_dir": str(tmp_path / "runs")},
    })


@pytest.fixture
def settings(settings_file):
    return load_settings(settings_file)


@pytest.fixture
def make_table(settings):
    """Build a TableConfig in code: make_table(column_mapping={...}, tolerance={...})."""
    def _make(**overrides):
        data = {
            "table_id": "t1",
            "source": {"connection": "legacy_databricks", "table": "t1"},
            "target": {"connection": "target_bigquery", "table": "t1"},
            **overrides,
        }
        return parse_table(data, settings, "test")
    return _make


def col(name: str, raw_type: str, dialect: str, nullable: bool | None = True) -> ColumnInfo:
    family, precision, scale = parse_type(raw_type, dialect)
    return ColumnInfo(name=name, raw_type=raw_type, family=family, nullable=nullable,
                      precision=precision, scale=scale)


def dbx(name, raw_type, nullable=True):
    return col(name, raw_type, "databricks", nullable)


def bq(name, raw_type, nullable=True):
    return col(name, raw_type, "bigquery", nullable)
