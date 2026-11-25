"""Comparison helpers that decide PASS/FAIL within configured tolerances.

Two kinds of harmless differences show up in every cross-engine migration:
  * floating point - SUM(DOUBLE) depends on the order rows are added, so
    1234.5600000001 vs 1234.56 must still pass.
  * timestamp formatting - '2026-01-05 10:00:00' (legacy string, IST) and
    '2026-01-05T04:30:00Z' (BigQuery, UTC) are the same instant.
These helpers normalise both sides before comparing.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation

DEFAULT_TIMESTAMP_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y/%m/%d %H:%M:%S",
    "%d-%m-%Y %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
    "%Y-%m-%d",
]


@dataclass
class Comparison:
    passed: bool
    difference: str | None
    detail: str = ""


# ---------------------------------------------------------------- counts

def compare_counts(source, target, abs_tol: float = 0, pct_tol: float = 0.0) -> Comparison:
    """Row counts and NULL counts: exact by default, optionally within N rows or N percent."""
    s, t = int(source), int(target)
    diff = t - s
    allowed = max(float(abs_tol), abs(s) * float(pct_tol) / 100.0)
    return Comparison(abs(diff) <= allowed, str(diff))


# --------------------------------------------------------------- numbers

def _to_number(value):
    if isinstance(value, bool):
        return Decimal(int(value))
    if isinstance(value, (int, Decimal)):
        return Decimal(value)
    if isinstance(value, float):
        return value
    if isinstance(value, str):
        try:
            return Decimal(value.strip())
        except InvalidOperation as exc:
            raise ValueError(f"'{value}' is not a number") from exc
    raise ValueError(f"Cannot compare value of type {type(value).__name__} as a number")


def _fmt_number(value) -> str:
    if isinstance(value, Decimal):
        text = format(value, "f")
        return text.rstrip("0").rstrip(".") if "." in text else text
    return f"{value:.12g}"


def compare_numeric(source, target, abs_tol: float, rel_tol: float) -> Comparison:
    """SUM / MIN / MAX. Passes when |target - source| <= max(abs_tol, rel_tol * max(|source|, |target|))."""
    if source is None and target is None:
        return Comparison(True, None, "both NULL")
    if source is None or target is None:
        return Comparison(False, None, "NULL on one side only")

    a, b = _to_number(source), _to_number(target)

    if isinstance(a, float) or isinstance(b, float):
        a, b = float(a), float(b)
        if math.isnan(a) or math.isnan(b):
            return Comparison(math.isnan(a) and math.isnan(b), "NaN", "NaN present")
        if math.isinf(a) or math.isinf(b):
            return Comparison(a == b, "inf", "infinity present")
        diff = b - a
        allowed = max(float(abs_tol), float(rel_tol) * max(abs(a), abs(b)))
        return Comparison(abs(diff) <= allowed, _fmt_number(diff))

    diff = b - a
    allowed = max(Decimal(str(abs_tol)), Decimal(str(rel_tol)) * max(abs(a), abs(b)))
    return Comparison(abs(diff) <= allowed, _fmt_number(diff))


# ------------------------------------------------------------ timestamps

def get_timezone(name: str | None):
    """'UTC' works everywhere; other names need the tzdata package on Windows."""
    if name is None or name.strip().upper() == "UTC":
        return timezone.utc
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(name)
    except Exception as exc:  # ZoneInfoNotFoundError, ModuleNotFoundError
        raise ValueError(
            f"Unknown time zone '{name}'. Check the spelling (e.g. Asia/Kolkata); "
            "on Windows also run: pip install tzdata"
        ) from exc


def _parse_timestamp_text(text: str, formats: list[str]) -> datetime:
    iso = text
    if iso[-1:] in ("Z", "z"):
        iso = iso[:-1] + "+00:00"
    elif iso.upper().endswith(" UTC"):
        iso = iso[:-4] + "+00:00"
    try:
        return datetime.fromisoformat(iso)
    except ValueError:
        pass
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse '{text}' as a timestamp; add its format to tolerance.timestamp_formats")


def to_utc(value, assume_timezone: str | None, formats: list[str] | None = None) -> datetime | None:
    """Turn a datetime, date or string into a timezone-aware UTC datetime.

    Values without a zone (naive datetimes, plain strings) are read in assume_timezone.
    """
    if value is None:
        return None
    if isinstance(value, datetime):          # check before date: datetime is a subclass of date
        dt = value
    elif isinstance(value, date):
        dt = datetime.combine(value, time.min)
    elif isinstance(value, str):
        dt = _parse_timestamp_text(value.strip(), formats or DEFAULT_TIMESTAMP_FORMATS)
    else:
        raise ValueError(f"Cannot compare value of type {type(value).__name__} as a timestamp")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=get_timezone(assume_timezone))
    return dt.astimezone(timezone.utc)


def to_date(value, formats: list[str] | None = None) -> date | None:
    """Calendar dates have no time zone: '2026-01-05' is the same date everywhere."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return _parse_timestamp_text(value.strip(), formats or DEFAULT_TIMESTAMP_FORMATS).date()
    raise ValueError(f"Cannot compare value of type {type(value).__name__} as a date")


def compare_dates(source, target, formats: list[str] | None = None) -> Comparison:
    """MIN / MAX of DATE columns: exact calendar-date match, no time zone shift."""
    if source is None and target is None:
        return Comparison(True, None, "both NULL")
    if source is None or target is None:
        return Comparison(False, None, "NULL on one side only")
    a, b = to_date(source, formats), to_date(target, formats)
    days = (b - a).days
    return Comparison(days == 0, f"{days}d", f"compared as dates: {a} vs {b}")


def compare_temporal(source, target, tol_seconds: float, source_tz: str, target_tz: str,
                     formats: list[str] | None = None) -> Comparison:
    """MIN / MAX of TIMESTAMP and DATETIME columns, after converting both sides to UTC."""
    if source is None and target is None:
        return Comparison(True, None, "both NULL")
    if source is None or target is None:
        return Comparison(False, None, "NULL on one side only")
    a = to_utc(source, source_tz, formats)
    b = to_utc(target, target_tz, formats)
    diff = (b - a).total_seconds()
    detail = f"compared in UTC: {a.isoformat()} vs {b.isoformat()}"
    return Comparison(abs(diff) <= float(tol_seconds), f"{diff:g}s", detail)
