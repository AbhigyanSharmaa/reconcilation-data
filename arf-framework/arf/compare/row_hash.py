"""Row hash validation: finds rows that are missing, extra or changed, even when every
count and SUM/MIN/MAX matches (e.g. a status changed from SHIPPED to CANCELLED).

How it works, in three steps that each move very little data:
  1. Buckets. Each side turns every row into canonical text (rules in arf/hashing.py),
     hashes it with MD5, and puts it in one of N buckets by its primary key. Per bucket
     it returns: row count + two sums of the row hashes. N rows come back per side.
  2. Drill-down. For buckets whose numbers differ, both sides return (key, row hash)
     for the rows in those buckets only. Python compares them key by key.
  3. Column diff. For a few changed rows, both sides return each column's canonical
     text, so the result names the columns that changed and shows both values.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from ..config import TableConfig
from ..connectors import Connector, HashColumn
from ..hashing import HASHABLE_FAMILIES, SEPARATOR, display
from ..models import CheckResult, ColumnInfo, Status
from ..type_mapping import DECIMAL
from . import ResultBuilder
from .schema import align_columns


@dataclass
class HashPlan:
    keys: list        # (source ColumnInfo, target ColumnInfo, decimal scale)
    columns: list     # (source ColumnInfo, target ColumnInfo, decimal scale)
    skipped: list     # (column label, reason)


def _label(src: ColumnInfo, tgt: ColumnInfo) -> str:
    return tgt.name if src.name.lower() == tgt.name.lower() else f"{src.name} -> {tgt.name}"


def _decimal_scale(table: TableConfig, src: ColumnInfo, tgt: ColumnInfo) -> int:
    if src.family != DECIMAL:
        return 0
    if table.hash.decimal_scale != "auto":
        return int(table.hash.decimal_scale)
    known = [s for s in (src.scale, tgt.scale) if s is not None]
    return min(known) if known else 9


def plan_hash(table: TableConfig, source_cols: list[ColumnInfo], target_cols: list[ColumnInfo]) -> HashPlan:
    """Decide which columns go into the row hash. Raises ValueError if the primary key is unusable."""
    pairs = align_columns(table, source_cols, target_cols).pairs
    by_source = {table.key(s.name): (s, t) for s, t in pairs}

    keys = []
    for name in table.primary_key:
        if name not in by_source:
            raise ValueError(f"primary_key column '{name}' was not found in both tables")
        s, t = by_source[name]
        if s.family != t.family or s.family not in HASHABLE_FAMILIES:
            raise ValueError(f"primary_key column '{name}' is {s.family} in source and {t.family} in target; "
                             "a key must have the same type on both sides")
        keys.append((s, t, _decimal_scale(table, s, t)))
    key_names = {table.key(s.name) for s, _t, _sc in keys}

    cfg = table.hash
    columns, skipped = [], []
    for s, t in pairs:
        if table.key(s.name) in key_names:
            continue
        label = _label(s, t)
        if cfg.columns != "auto" and table.key(t.name) not in cfg.columns:
            continue
        if table.key(t.name) in cfg.exclude_columns or table.key(s.name) in cfg.exclude_columns:
            skipped.append((label, "left out of the hash (hash.exclude_columns)"))
        elif s.family != t.family:
            skipped.append((label, f"left out of the hash: {s.family} vs {t.family} cannot produce identical "
                                   "text; the schema and numeric checks cover this column"))
        elif s.family not in HASHABLE_FAMILIES:
            skipped.append((label, f"left out of the hash: {s.family} columns are not hashed"))
        else:
            columns.append((s, t, _decimal_scale(table, s, t)))

    if cfg.columns != "auto":
        present = {table.key(t.name) for _s, t in pairs}
        for name in cfg.columns:
            if name not in present:
                skipped.append((name, "listed in hash.columns but not found in both tables"))
    return HashPlan(keys=keys, columns=columns, skipped=skipped)


def _side(plan_items: list, side: int, table: TableConfig) -> list[HashColumn]:
    cfg = table.hash
    return [HashColumn(name=item[side].name, family=item[side].family, scale=item[2],
                       float_digits=cfg.float_digits, trim=cfg.trim_strings) for item in plan_items]


def _sort_key(key_text: str):
    """Sort keys naturally: 9 before 10."""
    return [(0, int(p), "") if re.fullmatch(r"-?\d+", p) else (1, 0, p) for p in key_text.split(SEPARATOR)]


def run_row_hash(table: TableConfig, source: Connector, target: Connector, source_cols: list[ColumnInfo],
                 target_cols: list[ColumnInfo], rb: ResultBuilder) -> list[CheckResult]:
    plan = plan_hash(table, source_cols, target_cols)
    cfg = table.hash
    results = [rb.make("row_hash", "hash_columns", column=label, severity_key="row_hash",
                       status=Status.SKIPPED, message=reason) for label, reason in plan.skipped]

    src_keys, tgt_keys = _side(plan.keys, 0, table), _side(plan.keys, 1, table)
    src_cols, tgt_cols = _side(plan.columns, 0, table), _side(plan.columns, 1, table)
    key_names = [s.name for s, _t, _sc in plan.keys]

    def key_label(key_text: str) -> str:
        return ", ".join(f"{n}={display(v)}" for n, v in zip(key_names, key_text.split(SEPARATOR)))

    # 1. bucket summary
    sb = source.hash_buckets(table.source, src_keys, src_cols, cfg.buckets)
    tb = target.hash_buckets(table.target, tgt_keys, tgt_cols, cfg.buckets)
    differing = sorted(b for b in set(sb) | set(tb) if sb.get(b) != tb.get(b))
    src_total = sum(v[0] for v in sb.values())
    tgt_total = sum(v[0] for v in tb.values())
    hashed = f"key ({', '.join(key_names)}) + {len(plan.columns)} column(s)"
    results.append(rb.make(
        "row_hash", "bucket_summary", source=f"{src_total} rows", target=f"{tgt_total} rows",
        difference=f"{len(differing)} of {cfg.buckets} buckets differ", tolerance="exact",
        severity_key="row_hash", passed=not differing,
        message=f"every row matches ({hashed} hashed)" if not differing
        else f"some rows differ ({hashed} hashed); see row_differences"))
    if not differing:
        return results

    # 2. drill-down into the differing buckets
    inspect = differing[:cfg.max_buckets_to_inspect]
    src_rows = source.hash_rows(table.source, src_keys, src_cols, cfg.buckets, inspect)
    tgt_rows = target.hash_rows(table.target, tgt_keys, tgt_cols, cfg.buckets, inspect)
    missing = sorted(set(src_rows) - set(tgt_rows), key=_sort_key)
    extra = sorted(set(tgt_rows) - set(src_rows), key=_sort_key)
    changed = sorted((k for k in set(src_rows) & set(tgt_rows) if src_rows[k] != tgt_rows[k]), key=_sort_key)

    note = "" if len(inspect) == len(differing) else (
        f"; {len(differing) - len(inspect)} more differing bucket(s) not inspected "
        "(raise hash.max_buckets_to_inspect to see them)")
    results.append(rb.make(
        "row_hash", "row_differences", source=f"{len(missing)} missing", target=f"{len(extra)} extra",
        difference=f"{len(changed)} changed", severity_key="row_hash", passed=False,
        message=f"inspected {len(inspect)} of {len(differing)} differing bucket(s): {len(missing)} missing, "
                f"{len(extra)} extra, {len(changed)} changed row(s); up to {cfg.max_examples} of each listed{note}"))

    for k in missing[:cfg.max_examples]:
        results.append(rb.make("row_hash", "missing_row", column=key_label(k), source="present", target="absent",
                               severity_key="row_hash", passed=False,
                               message="row exists in source but not in target"))
    for k in extra[:cfg.max_examples]:
        results.append(rb.make("row_hash", "extra_row", column=key_label(k), source="absent", target="present",
                               severity_key="row_hash", passed=False,
                               message="row exists in target but not in source"))

    # 3. column diff for a few changed rows
    examples = changed[:cfg.max_examples]
    src_vals = source.hash_values(table.source, src_keys, src_cols, examples)
    tgt_vals = target.hash_values(table.target, tgt_keys, tgt_cols, examples)
    for k in examples:
        a, b = src_vals.get(k), tgt_vals.get(k)
        diffs = [] if a is None or b is None else [
            (s.name, t.name, x, y) for (s, t, _sc), x, y in zip(plan.columns, a, b) if x != y]
        if diffs:
            source_value = "; ".join(f"{sn}={display(x)}" for sn, _tn, x, _y in diffs)
            target_value = "; ".join(f"{tn}={display(y)}" for _sn, tn, _x, y in diffs)
            message = "changed column(s): " + ", ".join(tn for _sn, tn, _x, _y in diffs)
        else:
            source_value = target_value = None
            message = "row hash differs; the changed column could not be pinpointed"
        results.append(rb.make("row_hash", "changed_row", column=key_label(k), source=source_value,
                               target=target_value, severity_key="row_hash", passed=False, message=message))
    return results
