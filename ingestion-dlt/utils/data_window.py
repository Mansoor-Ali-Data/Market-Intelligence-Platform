from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone


@dataclass(frozen=True)
class ExtractionWindow:
    """
    Represents a deterministic UTC extraction window.

    The window is inclusive of the start boundary and exclusive
    of the end boundary.

    Example:

        2026-08-13T00:00:00Z
        →
        2026-08-14T00:00:00Z
    """

    start: str
    end: str


def build_daily_window(extraction_date: date) -> ExtractionWindow:
    """
    Build a one-day UTC extraction window.

    Args:
        extraction_date:
            Calendar date to extract.

    Returns:
        ExtractionWindow containing ISO-8601 UTC timestamps.
    """

    start_datetime = datetime.combine(
        extraction_date,
        time.min,
        tzinfo=timezone.utc,
    )

    end_datetime = start_datetime + timedelta(days=1)

    return ExtractionWindow(
        start=start_datetime.isoformat().replace("+00:00", "Z"),
        end=end_datetime.isoformat().replace("+00:00", "Z"),
    )