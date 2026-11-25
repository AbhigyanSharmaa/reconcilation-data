import pytest

from arf.config import ConfigError, load_settings, load_tables
from conftest import write_yaml


def table(table_id="orders", **extra):
    return {
        "table_id": table_id,
        "source": {"connection": "legacy_databricks", "table": table_id},
        "target": {"connection": "target_bigquery", "table": table_id},
        **extra,
    }


def test_defaults_are_applied(settings, tmp_path):
    write_yaml(tmp_path / "tables" / "orders.yaml", table())
    [t] = load_tables(tmp_path / "tables", settings)
    assert t.checks == ["schema", "row_count", "numeric"]
    assert t.tolerance.row_count_abs == 0
    assert t.severity["missing_column"] == "critical"
    assert t.numeric_columns == "auto"


def test_typo_in_key_is_rejected(settings, tmp_path):
    bad = table()
    bad["source"]["fliter"] = "x > 1"
    write_yaml(tmp_path / "t.yaml", bad)
    with pytest.raises(ConfigError, match="fliter"):
        load_tables(tmp_path / "t.yaml", settings)


def test_unknown_connection_is_rejected(settings, tmp_path):
    bad = table()
    bad["target"]["connection"] = "nope"
    write_yaml(tmp_path / "t.yaml", bad)
    with pytest.raises(ConfigError, match="nope"):
        load_tables(tmp_path / "t.yaml", settings)


def test_missing_table_id(settings, tmp_path):
    bad = table()
    del bad["table_id"]
    write_yaml(tmp_path / "t.yaml", bad)
    with pytest.raises(ConfigError, match="table_id"):
        load_tables(tmp_path / "t.yaml", settings)


def test_invalid_check_and_severity(settings, tmp_path):
    write_yaml(tmp_path / "a.yaml", table(checks=["row_count", "hash"]))
    with pytest.raises(ConfigError, match="hash"):
        load_tables(tmp_path / "a.yaml", settings)
    write_yaml(tmp_path / "b.yaml", table(severity={"row_count": "high"}))
    with pytest.raises(ConfigError, match="high"):
        load_tables(tmp_path / "b.yaml", settings)


def test_many_tables_in_one_file_and_only_filter(settings, tmp_path):
    write_yaml(tmp_path / "all.yaml", {"tables": [table("a"), table("b"), table("c", enabled=False)]})
    assert [t.table_id for t in load_tables(tmp_path / "all.yaml", settings)] == ["a", "b"]
    assert [t.table_id for t in load_tables(tmp_path / "all.yaml", settings, only=["b"])] == ["b"]
    with pytest.raises(ConfigError, match="zzz"):
        load_tables(tmp_path / "all.yaml", settings, only=["zzz"])


def test_duplicate_table_id(settings, tmp_path):
    write_yaml(tmp_path / "all.yaml", {"tables": [table("a"), table("a")]})
    with pytest.raises(ConfigError, match="Duplicate"):
        load_tables(tmp_path / "all.yaml", settings)


def test_tolerance_overrides_and_case_insensitive_names(settings, tmp_path):
    write_yaml(tmp_path / "t.yaml", table(
        tolerance={"row_count_abs": 5},
        column_tolerance={"Discount": {"numeric_abs": 0.01}},
        column_mapping={"Cust_ID": "Customer_ID"},
    ))
    [t] = load_tables(tmp_path / "t.yaml", settings)
    assert t.tolerance.row_count_abs == 5
    assert t.tolerance_for("DISCOUNT").numeric_abs == 0.01
    assert t.tolerance_for("DISCOUNT").row_count_abs == 5        # inherits the table override
    assert t.tolerance_for("amount").numeric_abs == 1e-6         # falls back to the default
    assert t.column_mapping == {"cust_id": "customer_id"}


def test_environment_variables_are_expanded(tmp_path, monkeypatch):
    monkeypatch.setenv("ARF_TEST_PROJECT", "my-proj")
    path = write_yaml(tmp_path / "s.yaml", {
        "connections": {"bq": {"type": "bigquery", "project": "${ARF_TEST_PROJECT}"}},
    })
    assert load_settings(path).connections["bq"]["project"] == "my-proj"


def test_bigquery_sink_requires_tables(tmp_path):
    path = write_yaml(tmp_path / "s.yaml", {
        "connections": {"bq": {"type": "bigquery"}},
        "output": {"sinks": ["bigquery"]},
    })
    with pytest.raises(ConfigError, match="results_table"):
        load_settings(path)


def test_scientific_notation_and_bad_numbers(settings, tmp_path):
    write_yaml(tmp_path / "a.yaml", table(tolerance={"numeric_abs": "1e-6"}))   # how YAML reads 1e-6
    [t] = load_tables(tmp_path / "a.yaml", settings)
    assert t.tolerance.numeric_abs == 1e-6 and t.tolerance.describe_numeric().startswith("abs<=1e-06")
    write_yaml(tmp_path / "b.yaml", table(tolerance={"row_count_pct": "lots"}))
    with pytest.raises(ConfigError, match="must be a number"):
        load_tables(tmp_path / "b.yaml", settings)
