"""Row count comparison (same filter applied on both sides)."""

from __future__ import annotations

from ..config import TableConfig
from ..connectors import Connector
from ..models import CheckResult
from ..tolerance import compare_counts
from . import ResultBuilder


def check_row_count(table: TableConfig, source: Connector, target: Connector, rb: ResultBuilder) -> CheckResult:
    src_count = source.row_count(table.source)
    tgt_count = target.row_count(table.target)
    tol = table.tolerance
    cmp = compare_counts(src_count, tgt_count, tol.row_count_abs, tol.row_count_pct)
    if cmp.passed:
        message = "match" if src_count == tgt_count else "within tolerance"
    else:
        message = f"target has {abs(tgt_count - src_count)} {'fewer' if tgt_count < src_count else 'more'} rows"
    return rb.make("row_count", "row_count", source=src_count, target=tgt_count, difference=cmp.difference,
                   tolerance=tol.describe_counts("row_count"), severity_key="row_count",
                   passed=cmp.passed, message=message)
