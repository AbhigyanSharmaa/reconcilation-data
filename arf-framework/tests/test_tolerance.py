from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from arf.tolerance import compare_counts, compare_dates, compare_numeric, compare_temporal


# ---- counts
def test_counts_exact_by_default():
    assert compare_counts(1000, 1000).passed
    result = compare_counts(1000, 997)
    assert not result.passed and result.difference == "-3"


def test_counts_with_abs_and_pct_tolerance():
    assert compare_counts(1000, 997, abs_tol=3).passed
    assert compare_counts(1000, 995, pct_tol=0.5).passed
    assert not compare_counts(1000, 994, pct_tol=0.5).passed


# ---- numbers
def test_float_noise_within_tolerance_passes():
    assert compare_numeric(1234.56, 1234.5600000001, abs_tol=1e-6, rel_tol=0).passed


def test_float_difference_outside_tolerance_fails():
    result = compare_numeric(1234.56, 1234.57, abs_tol=1e-6, rel_tol=0)
    assert not result.passed


def test_relative_tolerance_scales_with_size():
    assert compare_numeric(1e12, 1e12 + 500, abs_tol=0, rel_tol=1e-9).passed
    assert not compare_numeric(1e12, 1e12 + 5000, abs_tol=0, rel_tol=1e-9).passed


def test_decimal_exact_comparison():
    assert compare_numeric(Decimal("1316979.83"), Decimal("1316979.83"), 0, 0).passed
    result = compare_numeric(Decimal("100.00"), Decimal("105.00"), 0.01, 0)
    assert not result.passed and result.difference == "5"


def test_mixed_int_decimal_and_numeric_strings():
    assert compare_numeric(500500, Decimal("500500"), 0, 0).passed
    assert compare_numeric("12.50", Decimal("12.5"), 0, 0).passed


def test_nulls_and_nan():
    assert compare_numeric(None, None, 0, 0).passed
    assert not compare_numeric(None, 5, 0, 0).passed
    assert compare_numeric(float("nan"), float("nan"), 0, 0).passed
    assert not compare_numeric(float("nan"), 1.0, 0, 0).passed


def test_non_numeric_text_raises():
    with pytest.raises(ValueError):
        compare_numeric("abc", 1, 0, 0)


# ---- timestamps
def test_timestamp_formatting_difference_passes():
    assert compare_temporal("2026-01-05 10:00:00", "2026-01-05T10:00:00Z", 0, "UTC", "UTC").passed


def test_timestamp_timezone_difference_passes_when_configured():
    # legacy stored IST wall-clock, BigQuery stores UTC
    assert compare_temporal("2026-01-05 10:00:00", "2026-01-05T04:30:00Z", 0, "Asia/Kolkata", "UTC").passed
    assert not compare_temporal("2026-01-05 10:00:00", "2026-01-05T04:30:00Z", 0, "UTC", "UTC").passed


def test_timestamp_tolerance_seconds():
    a, b = "2026-01-05 10:00:00", "2026-01-05 10:00:01"
    assert not compare_temporal(a, b, 0, "UTC", "UTC").passed
    assert compare_temporal(a, b, 1, "UTC", "UTC").passed


def test_timestamp_microseconds_and_aware_datetimes():
    aware = datetime(2026, 1, 5, 10, 0, 0, 123456, tzinfo=timezone.utc)
    assert compare_temporal("2026-01-05 10:00:00.123456", aware, 0, "UTC", "UTC").passed


def test_custom_timestamp_format():
    fmts = ["%d-%m-%Y %H:%M:%S"]
    assert compare_temporal("05-01-2026 10:00:00", "2026-01-05T10:00:00Z", 0, "UTC", "UTC", fmts).passed


def test_unparseable_timestamp_raises():
    with pytest.raises(ValueError):
        compare_temporal("not a date", "2026-01-05", 0, "UTC", "UTC")


def test_unknown_timezone_gives_clear_error():
    with pytest.raises(ValueError, match="tzdata"):
        compare_temporal("2026-01-05 10:00:00", "2026-01-05 10:00:00", 0, "Mars/Olympus", "UTC")


# ---- dates
def test_dates_ignore_time_zones():
    assert compare_dates(date(2026, 1, 5), "2026-01-05").passed
    assert compare_dates(datetime(2026, 1, 5, 23, 59), date(2026, 1, 5)).passed
    result = compare_dates("2026-01-05", "2026-01-06")
    assert not result.passed and result.difference == "1d"
