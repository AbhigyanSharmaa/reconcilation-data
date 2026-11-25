"""Runs the configured checks for every table. One table failing never stops the others."""

from __future__ import annotations

import logging
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from .compare import ResultBuilder
from .compare.numeric import run_numeric
from .compare.row_hash import run_row_hash
from .compare.row_count import check_row_count
from .compare.schema import compare_schema
from .config import Settings, TableConfig
from .connectors import ConnectorPool
from .models import BLOCKING_STATUSES, CheckResult, Status
from .sinks import Sink, worst_status

log = logging.getLogger("arf")


def new_run_id(now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    return f"{now:%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:6]}"


@dataclass
class RunSummary:
    run_id: str
    run_dir: Path | None = None
    counts: Counter = field(default_factory=Counter)
    table_status: dict = field(default_factory=dict)
    sink_errors: list = field(default_factory=list)

    def add(self, table_id: str, results: list[CheckResult]) -> None:
        self.counts.update(r.status.value for r in results)
        self.table_status[table_id] = worst_status(results).value

    @property
    def ok(self) -> bool:
        blocking = any(self.counts[s.value] for s in BLOCKING_STATUSES)
        return not blocking and not self.sink_errors

    @property
    def exit_code(self) -> int:
        return 0 if self.ok else 1


class Runner:
    def __init__(self, settings: Settings, sinks: list[Sink], run_id: str | None = None,
                 run_dir: Path | None = None):
        self.settings = settings
        self.sinks = sinks
        self.run_ts = datetime.now(timezone.utc)
        self.run_id = run_id or new_run_id(self.run_ts)
        self.run_dir = run_dir
        self.pool = ConnectorPool(settings.connections)

    def run(self, tables: list[TableConfig]) -> RunSummary:
        summary = RunSummary(run_id=self.run_id, run_dir=self.run_dir)
        try:
            for table in tables:
                try:
                    results = self.run_table(table)
                except Exception as exc:   # safety net: an unexpected bug must not stop other tables
                    results = [ResultBuilder(self.run_id, self.run_ts.isoformat(), table)
                               .error("setup", "unexpected", exc)]
                summary.add(table.table_id, results)
                for sink in self.sinks:
                    sink.write(results)
        finally:
            self.pool.close_all()
            for sink in self.sinks:
                sink.close(summary)
        return summary

    def run_table(self, table: TableConfig) -> list[CheckResult]:
        rb = ResultBuilder(self.run_id, self.run_ts.isoformat(), table)
        log.info("[%s] %s (%s) -> %s (%s)", table.table_id, table.source.table, table.source.connection,
                 table.target.table, table.target.connection)
        results: list[CheckResult] = []

        try:
            source = self.pool.get(table.source.connection)
        except Exception as exc:
            return [rb.error("setup", "connect", exc, context=f"source connection '{table.source.connection}'")]
        try:
            target = self.pool.get(table.target.connection)
        except Exception as exc:
            return [rb.error("setup", "connect", exc, context=f"target connection '{table.target.connection}'")]

        source_cols = target_cols = None
        if {"schema", "numeric", "row_hash"} & set(table.checks):
            try:
                source_cols = source.get_schema(table.source)
            except Exception as exc:
                results.append(rb.error("schema", "schema_fetch", exc, context=f"source {table.source.table}"))
            try:
                target_cols = target.get_schema(table.target)
            except Exception as exc:
                results.append(rb.error("schema", "schema_fetch", exc, context=f"target {table.target.table}"))
        schemas_ok = source_cols is not None and target_cols is not None

        if "schema" in table.checks and schemas_ok:
            results.extend(compare_schema(table, source_cols, target_cols, rb))

        if "row_count" in table.checks:
            try:
                results.append(check_row_count(table, source, target, rb))
            except Exception as exc:
                results.append(rb.error("row_count", "row_count", exc))

        if "numeric" in table.checks:
            if not schemas_ok:
                results.append(rb.make("numeric", "profile", status=Status.SKIPPED, severity_key="numeric",
                                       message="Skipped because a schema could not be read (see schema_fetch)"))
            else:
                try:
                    results.extend(run_numeric(table, source, target, source_cols, target_cols, rb,
                                               self.settings.max_metrics_per_query))
                except Exception as exc:
                    results.append(rb.error("numeric", "profile", exc))

        if "row_hash" in table.checks:
            if not schemas_ok:
                results.append(rb.make("row_hash", "bucket_summary", status=Status.SKIPPED, severity_key="row_hash",
                                       message="Skipped because a schema could not be read (see schema_fetch)"))
            else:
                try:
                    results.extend(run_row_hash(table, source, target, source_cols, target_cols, rb))
                except Exception as exc:
                    results.append(rb.error("row_hash", "row_hash", exc))

        log.info("[%s] finished: %s", table.table_id, worst_status(results).value)
        return results
