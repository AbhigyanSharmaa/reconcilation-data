"""Plain data classes shared by every module."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum


class Status(str, Enum):
    PASS = "PASS"          # values match (within tolerance)
    WARN = "WARN"          # mismatch on a check marked severity: warning
    FAIL = "FAIL"          # mismatch on a check marked severity: critical
    ERROR = "ERROR"        # the check could not run (bad SQL, missing table, no access...)
    SKIPPED = "SKIPPED"    # check deliberately not run (e.g. type mismatch makes SUM meaningless)


# Statuses that go to the exceptions log.
EXCEPTION_STATUSES = {Status.WARN, Status.FAIL, Status.ERROR}

# Statuses that make the whole run fail (non-zero exit code, failed Airflow task).
BLOCKING_STATUSES = {Status.FAIL, Status.ERROR}


@dataclass
class ColumnInfo:
    """One column as seen by a connector, already normalised to a canonical type family."""

    name: str
    raw_type: str                 # type exactly as the engine reports it, e.g. DECIMAL(18,2), INT64
    family: str                   # canonical family, e.g. INTEGER, DECIMAL, TIMESTAMP (see type_mapping.py)
    nullable: bool | None = None  # None = engine did not tell us
    precision: int | None = None  # only for DECIMAL
    scale: int | None = None      # only for DECIMAL
    ordinal: int | None = None    # position in the table, 1-based


@dataclass
class CheckResult:
    """One row in arf_results (and arf_exceptions when status is WARN, FAIL or ERROR)."""

    run_id: str
    run_ts: str
    table_id: str
    source_table: str
    target_table: str
    check_type: str               # schema | row_count | numeric | setup
    check_name: str               # e.g. missing_column, data_type, row_count, sum, min, max, null_count
    column_name: str | None
    source_value: str | None
    target_value: str | None
    difference: str | None
    tolerance: str | None
    severity: str                 # critical | warning
    status: Status
    message: str
    error_detail: str | None = None   # traceback for ERROR rows

    def to_row(self) -> dict:
        row = asdict(self)
        row["status"] = self.status.value
        return row


RESULT_COLUMNS = [
    "run_id", "run_ts", "table_id", "source_table", "target_table",
    "check_type", "check_name", "column_name", "source_value", "target_value",
    "difference", "tolerance", "severity", "status", "message", "error_detail",
]


def to_text(value) -> str | None:
    """Store every value as text so one column can hold counts, sums, dates and type names."""
    if value is None:
        return None
    return str(value)
