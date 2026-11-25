"""Numeric aggregations: SUM, MIN, MAX per numeric column, MIN/MAX per date/timestamp column,
NULL counts per column.

Each side computes all its aggregates in one SELECT (one table scan), so only a
single row of numbers travels back to Python per side.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..config import TableConfig
from ..connectors import Connector, MetricSpec
from ..models import CheckResult, ColumnInfo, Status
from ..tolerance import compare_counts, compare_dates, compare_numeric, compare_temporal
from ..type_mapping import COMPLEX_FAMILIES, DATE, NUMERIC_FAMILIES, STRING, TEMPORAL_FAMILIES
from . import ResultBuilder
from .schema import align_columns


@dataclass
class PlannedMetric:
    alias: str
    metric: str          # sum | min | max | null_count
    kind: str            # numeric | temporal | date | count
    src: ColumnInfo
    tgt: ColumnInfo


@dataclass
class Plan:
    metrics: list
    skipped: list        # (column label, reason)
    not_found: list      # target column names listed in config but not present


def _label(src: ColumnInfo, tgt: ColumnInfo) -> str:
    return tgt.name if src.name.lower() == tgt.name.lower() else f"{src.name} -> {tgt.name}"


def _value_kind(src: ColumnInfo, tgt: ColumnInfo) -> str | None:
    """How SUM/MIN/MAX should be compared for this pair, or None if they cannot be."""
    if src.family in NUMERIC_FAMILIES and tgt.family in NUMERIC_FAMILIES:
        return "numeric"
    # A timestamp stored as STRING on one side is still compared after parsing it.
    allowed = TEMPORAL_FAMILIES | {STRING}
    if src.family in allowed and tgt.family in allowed and (
            src.family in TEMPORAL_FAMILIES or tgt.family in TEMPORAL_FAMILIES):
        # A calendar DATE has no time zone, so it is compared without UTC conversion.
        return "date" if DATE in (src.family, tgt.family) else "temporal"
    return None


def plan_metrics(table: TableConfig, source_cols: list[ColumnInfo], target_cols: list[ColumnInfo]) -> Plan:
    pairs = align_columns(table, source_cols, target_cols).pairs
    by_target = {table.key(t.name): (s, t) for s, t in pairs}

    if table.numeric_columns == "auto":
        profiled = [(s, t) for s, t in pairs
                    if t.family in NUMERIC_FAMILIES | TEMPORAL_FAMILIES
                    or s.family in NUMERIC_FAMILIES | TEMPORAL_FAMILIES]
        not_found = []
    else:
        profiled = [by_target[c] for c in table.numeric_columns if c in by_target]
        not_found = [c for c in table.numeric_columns if c not in by_target]

    metrics: list[PlannedMetric] = []
    skipped: list[tuple[str, str]] = []

    def add(metric: str, kind: str, s: ColumnInfo, t: ColumnInfo) -> None:
        metrics.append(PlannedMetric(f"m{len(metrics)}", metric, kind, s, t))

    for s, t in profiled:
        kind = _value_kind(s, t)
        if kind is None:
            skipped.append((_label(s, t), f"types {s.family} vs {t.family} cannot be compared "
                                          "with SUM/MIN/MAX - see the schema data_type check"))
        elif kind == "numeric":
            for m in ("sum", "min", "max"):
                add(m, kind, s, t)
        else:
            for m in ("min", "max"):
                add(m, kind, s, t)

    nc = table.null_count_columns
    if nc == "all":
        null_pairs = [(s, t) for s, t in pairs if s.family not in COMPLEX_FAMILIES and t.family not in COMPLEX_FAMILIES]
    elif nc == "profiled":
        null_pairs = profiled
    else:
        null_pairs = [by_target[c] for c in nc if c in by_target]
        not_found += [c for c in nc if c not in by_target and c not in not_found]
    for s, t in null_pairs:
        add("null_count", "count", s, t)

    return Plan(metrics=metrics, skipped=skipped, not_found=not_found)


def run_numeric(table: TableConfig, source: Connector, target: Connector, source_cols: list[ColumnInfo],
                target_cols: list[ColumnInfo], rb: ResultBuilder, chunk_size: int = 300) -> list[CheckResult]:
    plan = plan_metrics(table, source_cols, target_cols)
    results: list[CheckResult] = []

    for col in plan.not_found:
        results.append(rb.make("numeric", "column_lookup", column=col, severity_key="numeric", passed=False,
                               message="Column listed in numeric_columns/null_count_columns not found in both tables"))
    for label, reason in plan.skipped:
        results.append(rb.make("numeric", "sum_min_max", column=label, severity_key="numeric",
                               status=Status.SKIPPED, message=reason))
    if not plan.metrics:
        return results

    src_specs = [MetricSpec(m.alias, m.metric, m.src.name, m.src.family) for m in plan.metrics]
    tgt_specs = [MetricSpec(m.alias, m.metric, m.tgt.name, m.tgt.family) for m in plan.metrics]
    src_values = source.profile(table.source, src_specs, chunk_size)
    tgt_values = target.profile(table.target, tgt_specs, chunk_size)

    for m in plan.metrics:
        label = _label(m.src, m.tgt)
        tol = table.tolerance_for(m.tgt.name)
        sv, tv = src_values.get(m.alias), tgt_values.get(m.alias)
        try:
            if m.kind == "count":
                cmp = compare_counts(sv or 0, tv or 0, tol.null_count_abs, tol.null_count_pct)
                tol_text, sev = tol.describe_counts("null_count"), "null_count"
            elif m.kind == "numeric":
                cmp = compare_numeric(sv, tv, tol.numeric_abs, tol.numeric_rel)
                tol_text, sev = tol.describe_numeric(), "numeric"
            elif m.kind == "date":
                cmp = compare_dates(sv, tv, tol.timestamp_formats)
                tol_text, sev = "exact date", "numeric"
            else:
                cmp = compare_temporal(sv, tv, tol.timestamp_seconds, tol.source_timezone,
                                       tol.target_timezone, tol.timestamp_formats)
                tol_text, sev = tol.describe_temporal(), "numeric"
        except Exception as exc:   # e.g. a timestamp string in an unknown format
            results.append(rb.error("numeric", m.metric, exc, column=label))
            continue

        name = m.metric.upper().replace("_", " ")
        message = "match" if cmp.passed else f"{name} differs"
        if cmp.detail and not cmp.passed:
            message += f" ({cmp.detail})"
        results.append(rb.make("numeric", m.metric, column=label, source=sv, target=tv,
                               difference=cmp.difference, tolerance=tol_text, severity_key=sev,
                               passed=cmp.passed, message=message))
    return results
