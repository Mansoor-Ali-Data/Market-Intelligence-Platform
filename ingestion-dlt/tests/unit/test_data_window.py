"""
Unit tests for the data extraction window utilities.

Responsibilities
----------------
- Validate deterministic UTC daily extraction windows.
- Validate day-boundary calculations.
- Validate month-boundary handling.
- Validate year-boundary handling.
- Validate that each extraction window represents exactly one day.
"""

from datetime import date

from ingestion.utils.data_window import (
    ExtractionWindow,
    build_daily_window,
)


def test_build_daily_window_returns_expected_utc_boundaries() -> None:
    window = build_daily_window(date(2026, 9, 14))

    assert window == ExtractionWindow(
        start="2026-09-14T00:00:00Z",
        end="2026-09-15T00:00:00Z",
    )


def test_build_daily_window_handles_month_boundary() -> None:
    window = build_daily_window(date(2026, 9, 30))

    assert window.start == "2026-09-30T00:00:00Z"
    assert window.end == "2026-10-01T00:00:00Z"


def test_build_daily_window_handles_year_boundary() -> None:
    window = build_daily_window(date(2026, 12, 31))

    assert window.start == "2026-12-31T00:00:00Z"
    assert window.end == "2027-01-01T00:00:00Z"


def test_build_daily_window_is_exactly_one_day() -> None:
    window = build_daily_window(date(2026, 9, 14))

    assert window.start != window.end
    assert window.start == "2026-09-14T00:00:00Z"
    assert window.end == "2026-09-15T00:00:00Z"