"""Where results go. Every run writes:
  * arf_results    - every check, PASS or not (audit trail)
  * arf_exceptions - only WARN, FAIL and ERROR rows (what someone must look at)
to the console, CSV files and/or BigQuery, as listed in settings.yaml output.sinks.
"""

from __future__ import annotations

import csv
import logging
from collections import Counter
from pathlib import Path

from .models import EXCEPTION_STATUSES, RESULT_COLUMNS, CheckResult, Status

log = logging.getLogger("arf")

_ORDER = [Status.ERROR, Status.FAIL, Status.WARN, Status.PASS, Status.SKIPPED]


def worst_status(results: list[CheckResult]) -> Status:
    present = {r.status for r in results}
    for status in _ORDER:
        if status in present and status != Status.SKIPPED:
            return status
    return Status.SKIPPED


class Sink:
    def write(self, results: list[CheckResult]) -> None: ...
    def close(self, summary) -> None: ...


class ConsoleSink(Sink):
    def __init__(self, max_lines_per_table: int = 25):
        self.max_lines = max_lines_per_table

    def write(self, results: list[CheckResult]) -> None:
        if not results:
            return
        table_id = results[0].table_id
        counts = Counter(r.status.value for r in results)
        overall = worst_status(results).value
        breakdown = ", ".join(f"{counts[s.value]} {s.value}" for s in _ORDER if counts[s.value])
        print(f"\n{table_id:<30} {overall:<6} ({breakdown})")
        issues = [r for r in results if r.status in EXCEPTION_STATUSES]
        for r in issues[: self.max_lines]:
            where = f"{r.check_name}" + (f" [{r.column_name}]" if r.column_name else "")
            parts = [f"{label}={value}" for label, value in
                     (("source", r.source_value), ("target", r.target_value), ("diff", r.difference))
                     if value is not None]
            values = (" " + " ".join(parts)) if parts else ""
            print(f"   {r.status.value:<5} {where}{values} - {r.message}")
        if len(issues) > self.max_lines:
            print(f"   ... {len(issues) - self.max_lines} more in the exceptions file")

    def close(self, summary) -> None:
        c = summary.counts
        print("\n" + "=" * 70)
        print(f"ARF run {summary.run_id}: {len(summary.table_status)} table(s), "
              f"{c['PASS']} PASS, {c['WARN']} WARN, {c['FAIL']} FAIL, {c['ERROR']} ERROR, {c['SKIPPED']} SKIPPED")
        for table_id, status in summary.table_status.items():
            print(f"  {status:<6} {table_id}")
        if summary.run_dir:
            print(f"Results, exceptions and log: {summary.run_dir}")
        print("RESULT: " + ("PASSED" if summary.ok else "FAILED (see FAIL / ERROR rows above)"))


class CsvSink(Sink):
    def __init__(self, run_dir: Path):
        run_dir.mkdir(parents=True, exist_ok=True)
        self.results_path = run_dir / "arf_results.csv"
        self.exceptions_path = run_dir / "arf_exceptions.csv"
        self._files = []
        self._results = self._open(self.results_path)
        self._exceptions = self._open(self.exceptions_path)

    def _open(self, path: Path):
        fh = path.open("w", newline="", encoding="utf-8-sig")   # -sig so Excel detects UTF-8
        self._files.append(fh)
        writer = csv.DictWriter(fh, fieldnames=RESULT_COLUMNS)
        writer.writeheader()
        return writer

    def write(self, results: list[CheckResult]) -> None:
        for r in results:
            row = r.to_row()
            self._results.writerow(row)
            if r.status in EXCEPTION_STATUSES:
                self._exceptions.writerow(row)
        for fh in self._files:
            fh.flush()

    def close(self, summary) -> None:
        for fh in self._files:
            fh.close()


class BigQuerySink(Sink):
    """Buffers rows and appends them to arf_results / arf_exceptions at the end of the run."""

    def __init__(self, connector, results_table: str, exceptions_table: str):
        self.connector = connector
        self.results_table = results_table
        self.exceptions_table = exceptions_table
        self.rows: list[dict] = []
        self.exceptions: list[dict] = []

    def write(self, results: list[CheckResult]) -> None:
        for r in results:
            row = r.to_row()
            self.rows.append(row)
            if r.status in EXCEPTION_STATUSES:
                self.exceptions.append(row)

    def close(self, summary) -> None:
        try:
            self.connector.load_rows(self.results_table, self.rows)
            self.connector.load_rows(self.exceptions_table, self.exceptions)
            log.info("Wrote %d result rows and %d exception rows to BigQuery", len(self.rows), len(self.exceptions))
        except Exception as exc:
            summary.sink_errors.append(f"BigQuery sink: {exc}")
            log.error("Could not write results to BigQuery: %s", exc)
            log.debug("BigQuery sink traceback", exc_info=exc)
        finally:
            self.connector.close()
