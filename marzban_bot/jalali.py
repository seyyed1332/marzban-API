from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class JalaliDate:
    year: int
    month: int
    day: int

    def isoformat(self) -> str:
        return f"{self.year:04d}-{self.month:02d}-{self.day:02d}"


def _get_tz(tz_name: str):
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return UTC


def gregorian_to_jalali(gy: int, gm: int, gd: int) -> JalaliDate:
    g_d_m = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]

    if gy > 1600:
        jy = 979
        gy -= 1600
    else:
        jy = 0
        gy -= 621

    gy2 = gy + 1 if gm > 2 else gy
    days = (
        365 * gy
        + (gy2 + 3) // 4
        - (gy2 + 99) // 100
        + (gy2 + 399) // 400
        - 80
        + gd
        + g_d_m[gm - 1]
    )

    jy += 33 * (days // 12053)
    days %= 12053
    jy += 4 * (days // 1461)
    days %= 1461

    if days > 365:
        jy += (days - 1) // 365
        days = (days - 1) % 365

    if days < 186:
        jm = 1 + days // 31
        jd = 1 + (days % 31)
    else:
        jm = 7 + (days - 186) // 30
        jd = 1 + ((days - 186) % 30)

    return JalaliDate(year=int(jy), month=int(jm), day=int(jd))


def format_jalali_date(dt: datetime | None, tz_name: str) -> str:
    if dt is None:
        return "-"
    local = dt.astimezone(_get_tz(tz_name))
    j = gregorian_to_jalali(local.year, local.month, local.day)
    return j.isoformat()


def format_jalali_datetime(dt: datetime | None, tz_name: str) -> str:
    if dt is None:
        return "-"
    local = dt.astimezone(_get_tz(tz_name))
    j = gregorian_to_jalali(local.year, local.month, local.day)
    return f"{j.isoformat()} {local.strftime('%H:%M:%S')}"


_PERSIAN_DIGITS = str.maketrans("0123456789", "۰۱۲۳۴۵۶۷۸۹")


def format_tehran_hour(dt: datetime | None, tz_name: str) -> str:
    if dt is None:
        return "-"
    local = dt.astimezone(_get_tz(tz_name))
    hour = int(local.hour)
    hour12 = hour % 12
    if hour12 == 0:
        hour12 = 12
    minute = int(local.minute)

    if 0 <= hour <= 3:
        period = "بامداد"
    elif 4 <= hour <= 11:
        period = "صبح"
    elif hour == 12:
        period = "ظهر"
    elif 13 <= hour <= 16:
        period = "بعدازظهر"
    elif 17 <= hour <= 19:
        period = "عصر"
    else:
        period = "شب"

    hour_str = str(hour12).translate(_PERSIAN_DIGITS)
    minute_str = str(minute).rjust(2, "0").translate(_PERSIAN_DIGITS)
    return f"ساعت {hour_str}.{minute_str} {period}"
