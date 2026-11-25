"""Shared rules for row hash validation.

A row hash only works across two engines if both sides turn every value into
EXACTLY the same text before hashing. These are the rules each connector follows
(see hash_canonical() in databricks.py and bigquery.py). canonical_text() below is
the reference version in Python; the SQLite connector uses it directly.

  NULL       -> ~NULL~
  STRING     -> as stored (optionally trimmed)
  INTEGER    -> digits, e.g. 1001
  BOOLEAN    -> true / false
  DECIMAL    -> fixed number of decimals, e.g. 149.50 (scale = the smaller of the two sides)
  FLOAT      -> rounded to float_digits decimals, e.g. 0.123457
  DATE       -> 2026-01-05
  TIMESTAMP  -> 2026-01-05 04:30:00.000000 (in UTC)
  DATETIME   -> 2026-01-05 10:00:00.000000 (no zone)
Columns are joined with the ASCII unit separator (code 31), then hashed with MD5.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal

from .tolerance import to_date, to_utc
from .type_mapping import BOOLEAN, DATE, DATETIME, DECIMAL, FLOAT, INTEGER, STRING, TIMESTAMP

NULL_TOKEN = "~NULL~"
SEPARATOR = "\x1f"
HASHABLE_FAMILIES = {STRING, INTEGER, BOOLEAN, DECIMAL, FLOAT, DATE, TIMESTAMP, DATETIME}
TS_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


@dataclass
class HashColumn:
    """One column fed into the row hash, as it exists on ONE side."""

    name: str
    family: str
    scale: int = 0          # decimals kept for DECIMAL columns
    float_digits: int = 6   # decimals kept for FLOAT columns
    trim: bool = False      # strip spaces from text


def canonical_text(value, family: str, scale: int = 0, float_digits: int = 6, trim: bool = False) -> str:
    """Reference implementation of the canonical text rules above."""
    if value is None:
        return NULL_TOKEN
    if family == STRING:
        text = str(value)
        return text.strip() if trim else text
    if family == INTEGER:
        return str(int(Decimal(str(value))))
    if family == BOOLEAN:
        return "true" if str(value).strip().lower() in ("1", "true") else "false"
    if family == DECIMAL:
        quantum = Decimal(1).scaleb(-scale)
        return f"{Decimal(str(value)).quantize(quantum, rounding=ROUND_HALF_UP):f}"
    if family == FLOAT:
        return f"{float(value):.{float_digits}f}"
    if family == DATE:
        return to_date(value).isoformat()
    if family == TIMESTAMP:
        return to_utc(value, "UTC").strftime(TS_FORMAT)
    if family == DATETIME:
        dt = value if isinstance(value, datetime) else to_utc(value, "UTC")
        return dt.replace(tzinfo=None).strftime(TS_FORMAT)
    raise ValueError(f"{family} values cannot be hashed")


def display(text: str) -> str:
    """How a canonical value is shown in results: NULL instead of the token."""
    return "NULL" if text == NULL_TOKEN else text
