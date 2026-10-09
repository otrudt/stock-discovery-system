"""US equity session times. Knowledge time is always stored in UTC."""

from __future__ import annotations

from datetime import UTC, date, datetime, time
from zoneinfo import ZoneInfo

NEW_YORK = ZoneInfo("America/New_York")
SESSION_OPEN = time(9, 30)
SESSION_CLOSE = time(16, 0)


def market_open_utc(day: date) -> datetime:
    return datetime.combine(day, SESSION_OPEN, tzinfo=NEW_YORK).astimezone(UTC)


def market_close_utc(day: date) -> datetime:
    """When a session's closing price becomes known. Ignores half-day closes, which
    only make this later than reality — the safe direction for look-ahead."""
    return datetime.combine(day, SESSION_CLOSE, tzinfo=NEW_YORK).astimezone(UTC)


def utc_now() -> datetime:
    return datetime.now(UTC)


def require_aware(ts: datetime) -> datetime:
    """Naive timestamps are ambiguous about when something was known; refuse them."""
    if ts.tzinfo is None:
        raise ValueError(f"timestamp must be timezone-aware: {ts!r}")
    return ts.astimezone(UTC)
