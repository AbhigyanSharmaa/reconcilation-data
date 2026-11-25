"""WBS 1.4 acceptance test: run ARF on sample source/target tables with planted defects
and verify that every defect is detected and written to the exceptions log."""

import csv

import pytest

from arf.cli import execute, main
from arf.config import load_settings, load_tables
from arf.demo import DEMO_CONFIG, build_demo_databases
from conftest import write_yaml


@pytest.fixture
def demo_run(tmp_path, settings_file):
    build_demo_databases(tmp_path)          # same folder the settings_file connections point to
    settings = load_settings(settings_file)
    tables = load_tables(DEMO_CONFIG / "tables", settings)
    summary = execute(settings, tables, ["csv"], run_id="test_run")
    run_dir = tmp_path / "runs" / "test_run"
    with open(run_dir / "arf_results.csv", encoding="utf-8-sig") as fh:
        results = list(csv.DictReader(fh))
    with open(run_dir / "arf_exceptions.csv", encoding="utf-8-sig") as fh:
        exceptions = list(csv.DictReader(fh))
    return summary, results, exceptions, run_dir


def find(rows, table, check, column=""):
    return [r for r in rows if r["table_id"] == table and r["check_name"] == check and r["column_name"] == column]


def test_overall_outcome(demo_run):
    summary, *_ = demo_run
    assert summary.table_status == {"customers": "PASS", "inventory": "ERROR", "orders": "FAIL"}
    assert summary.exit_code == 1


def test_format_timezone_and_float_noise_do_not_cause_failures(demo_run):
    _, results, _, _ = demo_run
    customers = [r for r in results if r["table_id"] == "customers"]
    assert customers and all(r["status"] in ("PASS", "SKIPPED") for r in customers)
    # the only SKIPPED row: created_ts is deliberately left out of the row hash (IST text vs UTC)
    assert [r["column_name"] for r in customers if r["status"] == "SKIPPED"] == ["created_ts"]
    assert find(results, "customers", "min", "created_ts")[0]["status"] == "PASS"   # IST text vs UTC ISO
    assert find(results, "customers", "sum", "credit_limit")[0]["status"] == "PASS"  # 1e-9 float noise


@pytest.mark.parametrize("check, column, status", [
    ("row_count", "", "FAIL"),
    ("missing_column", "legacy_flag", "FAIL"),
    ("extra_column", "load_ts", "WARN"),
    ("data_type", "quantity", "FAIL"),
    ("nullability", "order_id", "WARN"),
    ("decimal_precision", "unit_price", "WARN"),
    ("null_count", "cust_id -> customer_id", "FAIL"),
    ("sum", "amount", "FAIL"),
])
def test_planted_defects_are_detected_and_logged(demo_run, check, column, status):
    _, results, exceptions, _ = demo_run
    [row] = find(results, "orders", check, column)
    assert row["status"] == status
    assert find(exceptions, "orders", check, column), "defect missing from the exceptions log"


def test_row_count_values(demo_run):
    _, results, _, _ = demo_run
    [row] = find(results, "orders", "row_count")
    assert (row["source_value"], row["target_value"], row["difference"]) == ("1000", "997", "-3")


def test_exceptions_file_holds_exactly_the_problem_rows(demo_run):
    _, results, exceptions, _ = demo_run
    expected = [r for r in results if r["status"] in ("WARN", "FAIL", "ERROR")]
    assert exceptions == expected
    assert not any(r["status"] == "PASS" for r in exceptions)


def test_missing_table_is_an_error_with_traceback_and_run_continues(demo_run):
    _, results, exceptions, run_dir = demo_run
    errors = [r for r in exceptions if r["table_id"] == "inventory" and r["status"] == "ERROR"]
    assert errors and all("Traceback" in r["error_detail"] for r in errors)
    assert "TableNotFoundError" in (run_dir / "arf.log").read_text(encoding="utf-8")
    assert any(r["table_id"] == "orders" for r in results)   # later tables still ran


def test_tolerance_can_accept_a_known_difference(tmp_path, settings_file):
    build_demo_databases(tmp_path)
    settings = load_settings(settings_file)
    write_yaml(tmp_path / "orders.yaml", {
        "table_id": "orders", "checks": ["row_count"],
        "source": {"connection": "legacy_databricks", "table": "orders"},
        "target": {"connection": "target_bigquery", "table": "orders"},
        "tolerance": {"row_count_abs": 3},
    })
    summary = execute(settings, load_tables(tmp_path / "orders.yaml", settings), ["csv"])
    assert summary.table_status == {"orders": "PASS"}


def test_cli_exit_codes(tmp_path, settings_file, capsys):
    build_demo_databases(tmp_path)
    args = ["--settings", str(settings_file), "--tables", str(DEMO_CONFIG / "tables")]
    assert main(["check-config", *args]) == 0
    assert main(["run", *args, "--only", "customers"]) == 0      # clean table -> success
    assert main(["run", *args]) == 1                              # FAIL/ERROR present -> failure
    write_yaml(tmp_path / "bad.yaml", {"table_id": "x", "source": {"connection": "nope", "table": "t"}})
    assert main(["run", "--settings", str(settings_file), "--tables", str(tmp_path / "bad.yaml")]) == 2


def test_broken_connection_is_an_error_for_each_table_not_a_crash(tmp_path):
    settings_path = write_yaml(tmp_path / "s.yaml", {
        "connections": {
            "dbx": {"type": "databricks"},                      # no server_hostname -> cannot connect
            "tgt": {"type": "sqlite", "path": str(tmp_path / "missing.db"), "emulate": "bigquery"},
        },
        "output": {"sinks": ["csv"], "output_dir": str(tmp_path / "runs")},
    })
    settings = load_settings(settings_path)
    tables_path = write_yaml(tmp_path / "t.yaml", {"tables": [
        {"table_id": name, "source": {"connection": "dbx", "table": f"s.{name}"},
         "target": {"connection": "tgt", "table": name}} for name in ("a", "b")]})
    summary = execute(settings, load_tables(tables_path, settings), ["csv"])
    assert summary.table_status == {"a": "ERROR", "b": "ERROR"}
    assert summary.exit_code == 1
