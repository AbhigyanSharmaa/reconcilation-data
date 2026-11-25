"""Schema alignment: column names, data types, nullability, decimal precision."""

from __future__ import annotations

from dataclasses import dataclass

from ..config import TableConfig
from ..models import CheckResult, ColumnInfo, Status
from ..type_mapping import DECIMAL, is_compatible
from . import ResultBuilder


@dataclass
class Alignment:
    pairs: list          # (source ColumnInfo, target ColumnInfo) matched by name / column_mapping
    missing: list        # source columns with no target column
    extra: list          # target columns with no source column


def align_columns(table: TableConfig, source_cols: list[ColumnInfo], target_cols: list[ColumnInfo]) -> Alignment:
    """Match source columns to target columns, applying column_mapping and ignore_columns."""
    ignored = set(table.ignore_columns)
    targets = {table.key(c.name): c for c in target_cols}
    used: set[str] = set()
    pairs, missing = [], []

    for src in source_cols:
        src_key = table.key(src.name)
        tgt_key = table.column_mapping.get(src_key, src_key)
        if src_key in ignored or tgt_key in ignored:
            continue
        tgt = targets.get(tgt_key)
        if tgt is None:
            missing.append(src)
        else:
            pairs.append((src, tgt))
            used.add(tgt_key)

    extra = [c for key, c in targets.items() if key not in used and key not in ignored]
    return Alignment(pairs=pairs, missing=missing, extra=extra)


def _label(src: ColumnInfo, tgt: ColumnInfo) -> str:
    return tgt.name if src.name.lower() == tgt.name.lower() else f"{src.name} -> {tgt.name}"


def compare_schema(table: TableConfig, source_cols: list[ColumnInfo], target_cols: list[ColumnInfo],
                   rb: ResultBuilder) -> list[CheckResult]:
    al = align_columns(table, source_cols, target_cols)
    results: list[CheckResult] = []

    for src in al.missing:
        results.append(rb.make("schema", "missing_column", column=src.name, source=src.raw_type,
                               severity_key="missing_column", passed=False,
                               message="Column exists in source but not in target "
                                       "(add to column_mapping if it was renamed)"))
    for tgt in al.extra:
        results.append(rb.make("schema", "extra_column", column=tgt.name, target=tgt.raw_type,
                               severity_key="extra_column", passed=False,
                               message="Column exists in target but not in source"))

    for src, tgt in al.pairs:
        col = _label(src, tgt)

        ok = is_compatible(src.family, tgt.family, table.allowed_type_changes)
        results.append(rb.make("schema", "data_type", column=col, source=src.raw_type, target=tgt.raw_type,
                               severity_key="data_type", passed=ok,
                               message=f"{src.family} vs {tgt.family}" + ("" if ok else " - type family differs")))

        if src.nullable is None or tgt.nullable is None:
            results.append(rb.make("schema", "nullability", column=col, source=src.nullable, target=tgt.nullable,
                                   severity_key="nullability", status=Status.SKIPPED,
                                   message="Nullability not reported by one side"))
        else:
            same = src.nullable == tgt.nullable
            results.append(rb.make(
                "schema", "nullability", column=col,
                source="NULLABLE" if src.nullable else "NOT NULL",
                target="NULLABLE" if tgt.nullable else "NOT NULL",
                severity_key="nullability", passed=same,
                message="match" if same else "NOT NULL constraint differs"))

        if src.family == DECIMAL and tgt.family == DECIMAL and None not in (
                src.precision, src.scale, tgt.precision, tgt.scale):
            problems = []
            if tgt.scale < src.scale:
                problems.append(f"scale reduced {src.scale} -> {tgt.scale} (decimals will be rounded)")
            if (tgt.precision - tgt.scale) < (src.precision - src.scale):
                problems.append(f"integer digits reduced {src.precision - src.scale} -> "
                                f"{tgt.precision - tgt.scale} (large values may overflow)")
            results.append(rb.make(
                "schema", "decimal_precision", column=col,
                source=f"({src.precision},{src.scale})", target=f"({tgt.precision},{tgt.scale})",
                severity_key="decimal_precision", passed=not problems,
                message="; ".join(problems) if problems else "target can hold every source value"))

    return results
