from arf.compare import ResultBuilder
from arf.compare.schema import compare_schema
from conftest import bq, dbx


def run(table, source_cols, target_cols):
    rb = ResultBuilder("run1", "2026-11-24T00:00:00+00:00", table)
    return compare_schema(table, source_cols, target_cols, rb)


def statuses(results):
    return {(r.check_name, r.column_name): r.status.value for r in results}


def test_equivalent_types_pass(make_table):
    src = [dbx("id", "BIGINT"), dbx("amt", "DECIMAL(18,2)"), dbx("ts", "TIMESTAMP"), dbx("ok", "BOOLEAN")]
    tgt = [bq("id", "INT64"), bq("amt", "NUMERIC"), bq("ts", "TIMESTAMP"), bq("ok", "BOOL")]
    results = run(make_table(), src, tgt)
    assert all(r.status.value == "PASS" for r in results), [r.message for r in results if r.status.value != "PASS"]


def test_missing_and_extra_columns(make_table):
    results = run(make_table(), [dbx("id", "BIGINT"), dbx("old", "STRING")],
                  [bq("id", "INT64"), bq("new", "STRING")])
    s = statuses(results)
    assert s[("missing_column", "old")] == "FAIL"     # critical by default
    assert s[("extra_column", "new")] == "WARN"       # warning by default


def test_renamed_column_via_mapping(make_table):
    table = make_table(column_mapping={"cust_id": "customer_id"})
    results = run(table, [dbx("cust_id", "BIGINT")], [bq("customer_id", "INT64")])
    s = statuses(results)
    assert s[("data_type", "cust_id -> customer_id")] == "PASS"
    assert not any(r.check_name in ("missing_column", "extra_column") for r in results)


def test_type_mismatch_and_allowed_change(make_table):
    results = run(make_table(), [dbx("qty", "INT")], [bq("qty", "STRING")])
    assert statuses(results)[("data_type", "qty")] == "FAIL"

    allowed = make_table(allowed_type_changes=[["INTEGER", "STRING"]])
    assert statuses(run(allowed, [dbx("qty", "INT")], [bq("qty", "STRING")]))[("data_type", "qty")] == "PASS"


def test_nullability_difference_is_a_warning(make_table):
    results = run(make_table(), [dbx("id", "BIGINT", nullable=False)], [bq("id", "INT64", nullable=True)])
    assert statuses(results)[("nullability", "id")] == "WARN"


def test_nullability_unknown_is_skipped(make_table):
    results = run(make_table(), [dbx("id", "BIGINT", nullable=None)], [bq("id", "INT64")])
    assert statuses(results)[("nullability", "id")] == "SKIPPED"


def test_decimal_scale_and_integer_digits(make_table):
    results = run(make_table(),
                  [dbx("price", "DECIMAL(38,12)"), dbx("big", "DECIMAL(38,0)")],
                  [bq("price", "NUMERIC"), bq("big", "NUMERIC")])
    s = statuses(results)
    assert s[("decimal_precision", "price")] == "WARN"   # scale 12 -> 9
    assert s[("decimal_precision", "big")] == "WARN"     # 38 integer digits -> 29


def test_ignore_columns_and_case_insensitive_names(make_table):
    table = make_table(ignore_columns=["_rescued_data", "load_ts"])
    results = run(table,
                  [dbx("ORDER_ID", "BIGINT"), dbx("_rescued_data", "STRING")],
                  [bq("order_id", "INT64"), bq("load_ts", "TIMESTAMP")])
    assert all(r.status.value == "PASS" for r in results)


def test_severity_override_turns_fail_into_warn(make_table):
    table = make_table(severity={"missing_column": "warning"})
    results = run(table, [dbx("id", "BIGINT"), dbx("old", "STRING")], [bq("id", "INT64")])
    assert statuses(results)[("missing_column", "old")] == "WARN"
