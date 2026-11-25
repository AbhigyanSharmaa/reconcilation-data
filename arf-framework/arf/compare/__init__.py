"""Comparison modules: schema alignment, row counts, numeric aggregates."""

from __future__ import annotations

import logging
import traceback

from ..config import TableConfig
from ..models import CheckResult, Status, to_text

log = logging.getLogger("arf")


class ResultBuilder:
    """Creates CheckResult rows for one table so every module fills the same columns."""

    def __init__(self, run_id: str, run_ts: str, table: TableConfig):
        self.run_id = run_id
        self.run_ts = run_ts
        self.table = table

    def make(self, check_type: str, check_name: str, *, column: str | None = None, source=None, target=None,
             difference=None, tolerance: str | None = None, severity_key: str | None = None,
             passed: bool | None = None, status: Status | None = None, message: str = "",
             error_detail: str | None = None) -> CheckResult:
        severity = self.table.severity.get(severity_key or check_type, "critical")
        if status is None:
            if passed:
                status = Status.PASS
            else:
                status = Status.FAIL if severity == "critical" else Status.WARN
        return CheckResult(
            run_id=self.run_id,
            run_ts=self.run_ts,
            table_id=self.table.table_id,
            source_table=self.table.source.table,
            target_table=self.table.target.table,
            check_type=check_type,
            check_name=check_name,
            column_name=column,
            source_value=to_text(source),
            target_value=to_text(target),
            difference=to_text(difference),
            tolerance=tolerance,
            severity=severity,
            status=status,
            message=message,
            error_detail=error_detail,
        )

    def error(self, check_type: str, check_name: str, exc: Exception, *, column: str | None = None,
              context: str = "") -> CheckResult:
        """A check that could not run. Logged with full traceback, recorded as ERROR, run continues."""
        prefix = f"{context}: " if context else ""
        message = f"{prefix}{type(exc).__name__}: {exc}"
        # result_row=True lets the console hide this line when the console sink already prints it.
        log.error("[%s] %s/%s failed - %s", self.table.table_id, check_type, check_name, message,
                  extra={"result_row": True})
        log.debug("Traceback for %s %s/%s", self.table.table_id, check_type, check_name, exc_info=exc)
        detail = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        return CheckResult(
            run_id=self.run_id, run_ts=self.run_ts, table_id=self.table.table_id,
            source_table=self.table.source.table, target_table=self.table.target.table,
            check_type=check_type, check_name=check_name, column_name=column,
            source_value=None, target_value=None, difference=None, tolerance=None,
            severity="critical", status=Status.ERROR, message=message, error_detail=detail,
        )
