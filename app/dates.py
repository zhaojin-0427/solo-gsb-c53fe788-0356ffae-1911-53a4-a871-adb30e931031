"""HTTP-date parsing / formatting (RFC 9110, Section 5.6.7).

Only the IMF-fixdate format is produced; the three accepted historical
formats are tolerated on input.  Everything is anchored to UTC so that
Last-Modified values have stable second precision.
"""
from __future__ import annotations

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

IMF_FIXDATE = "%a, %d %b %Y %H:%M:%S GMT"


def http_date(dt: datetime) -> str:
    """Format a datetime as an IMF-fixdate (always GMT, second precision)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime(IMF_FIXDATE)


def parse_http_date(value: str) -> datetime | None:
    """Parse an HTTP-date.

    Returns a timezone-aware UTC datetime, or ``None`` when the header is
    not a syntactically valid HTTP-date (per RFC, such a header must be
    ignored rather than treated as a failed precondition).
    """
    try:
        dt = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    if dt is None or dt.tzinfo is None:
        return None
    return dt.astimezone(timezone.utc)
