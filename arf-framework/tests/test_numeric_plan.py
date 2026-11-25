from arf.compare.numeric import plan_metrics
from conftest import bq, dbx

SOURCE = [dbx("id", "BIGINT"), dbx("amount", "DECIMAL(18,2)"), dbx("name", "STRING"),
          dbx("order_ts", "TIMESTAMP"), dbx("order_date", "DATE"), dbx("qty", "INT"),
          dbx("tags", "ARRAY<STRING>")]
TARGET = [bq("id", "INT64"), bq("amount", "NUMERIC"), bq("name", "STRING"),
          bq("order_ts", "TIMESTAMP"), bq("order_date", "DATE"), bq("qty", "STRING"),
          bq("tags", "ARRAY<STRING>")]


def metrics(plan):
    return {(m.metric, m.tgt.name): m.kind for m in plan.metrics}


def test_auto_profiles_numeric_and_temporal_columns(make_table):
    plan = plan_metrics(make_table(), SOURCE, TARGET)
    m = metrics(plan)
    assert m[("sum", "amount")] == "numeric" and ("max", "id") in m
    assert m[("min", "order_ts")] == "temporal"
    assert m[("max", "order_date")] == "date"           # dates are compared without time zones
    assert ("sum", "name") not in m                     # strings get null counts only
    assert ("null_count", "name") in m
    assert ("null_count", "tags") not in m              # nested types are skipped


def test_type_mismatch_is_skipped_not_summed(make_table):
    plan = plan_metrics(make_table(), SOURCE, TARGET)
    assert ("sum", "qty") not in metrics(plan)
    assert [label for label, _ in plan.skipped] == ["qty"]


def test_explicit_column_list(make_table):
    plan = plan_metrics(make_table(numeric_columns=["amount", "missing_col"], null_count_columns="profiled"),
                        SOURCE, TARGET)
    assert set(metrics(plan)) == {("sum", "amount"), ("min", "amount"), ("max", "amount"),
                                  ("null_count", "amount")}
    assert plan.not_found == ["missing_col"]


def test_string_timestamp_on_one_side_is_still_compared(make_table):
    plan = plan_metrics(make_table(), [dbx("ts", "STRING")], [bq("ts", "TIMESTAMP")])
    assert metrics(plan)[("min", "ts")] == "temporal"
