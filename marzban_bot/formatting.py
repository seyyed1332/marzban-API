from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from zoneinfo import ZoneInfo


def get_tz(tz_name: str):
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return UTC


def format_bytes(num: int | None) -> str:
    if num is None:
        return "-"
    if num < 0:
        return str(num)
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    value = float(num)
    unit = units[0]
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            break
        value /= 1024.0
    if unit == "B":
        return f"{int(value)} {unit}"
    return f"{value:.2f} {unit}"


def format_dt(value: datetime | None, tz_name: str) -> str:
    if value is None:
        return "-"
    tz = get_tz(tz_name)
    return value.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S %Z")


def parse_iso_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        # FastAPI returns ISO datetime strings.
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def parse_epoch_seconds(value: int | None) -> datetime | None:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=UTC)
    except Exception:
        return None


@dataclass(frozen=True)
class ScheduleInfo:
    interval_hours: int
    next_run_at: datetime
