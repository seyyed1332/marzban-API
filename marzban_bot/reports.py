from __future__ import annotations

import io
from datetime import UTC, datetime
from typing import Any

from telegram import InputFile

from .formatting import format_bytes, format_dt, parse_epoch_seconds, parse_iso_dt
from .subscription import resolve_subscription_to_links


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def build_report_message(
    *,
    user: dict[str, Any],
    usage: dict[str, Any] | None,
    tz_name: str,
    now: datetime,
    next_reset_at: datetime | None,
    interval_hours: int | None,
    reason: str,
) -> str:
    username = str(user.get("username", "-"))
    status = str(user.get("status", "-"))

    used = _safe_int(user.get("used_traffic")) or 0
    data_limit = _safe_int(user.get("data_limit"))
    if not data_limit or data_limit <= 0:
        data_limit = None

    if data_limit is not None and data_limit > 0:
        pct = used / data_limit * 100.0
        traffic_line = f"{format_bytes(used)} / {format_bytes(data_limit)} ({pct:.1f}%)"
    else:
        traffic_line = f"{format_bytes(used)} / {format_bytes(data_limit)}"

    expire_dt = parse_epoch_seconds(_safe_int(user.get("expire")))
    created_dt = parse_iso_dt(user.get("created_at"))
    sub_updated_dt = parse_iso_dt(user.get("sub_updated_at"))

    subscription_url = str(user.get("subscription_url", "")).strip()

    lines = [
        f"Reason: {reason}",
        f"Time: {format_dt(now, tz_name)}",
        f"Username: {username}",
        f"Status: {status}",
        f"Traffic: {traffic_line}",
        f"Created: {format_dt(created_dt, tz_name)}",
        f"Expire: {format_dt(expire_dt, tz_name)}",
        f"Sub updated: {format_dt(sub_updated_dt, tz_name)}",
    ]

    if interval_hours is not None and next_reset_at is not None:
        lines.append(f"Next reset: {format_dt(next_reset_at, tz_name)} (every {interval_hours}h)")
    elif next_reset_at is not None:
        lines.append(f"Next reset: {format_dt(next_reset_at, tz_name)}")

    if usage and isinstance(usage.get("usages"), list):
        usages = usage.get("usages") or []
        if usages:
            lines.append("Node usage:")
            for item in usages[:10]:
                node = str(item.get("node_name", "-"))
                node_used = _safe_int(item.get("used_traffic")) or 0
                lines.append(f"- {node}: {format_bytes(node_used)}")

    return "\n".join(lines)


def build_links_document(
    *,
    user: dict[str, Any],
    resolved_links: list[str],
    tz_name: str,
    now: datetime,
    next_reset_at: datetime | None,
) -> InputFile:
    username = str(user.get("username", "user"))
    subscription_url = str(user.get("subscription_url", "")).strip()

    header_lines = [
        f"username={username}",
        f"generated_at={format_dt(now, tz_name)}",
    ]
    if next_reset_at is not None:
        header_lines.append(f"next_reset_at={format_dt(next_reset_at, tz_name)}")
    if subscription_url:
        header_lines.append(f"subscription_url={subscription_url}")

    content = "\n".join(header_lines) + "\n\n" + "\n".join(resolved_links) + "\n"
    bio = io.BytesIO(content.encode("utf-8"))
    bio.name = f"configs_{username}.txt"
    return InputFile(bio)


def resolve_links_from_api_user(user: dict[str, Any]) -> list[str]:
    raw = user.get("links")
    if isinstance(raw, list) and all(isinstance(x, str) for x in raw):
        links = [x.strip() for x in raw if x and x.strip()]
        if links:
            return links
    return []


def resolve_links_from_subscription_payload(payload_text: str) -> list[str]:
    return resolve_subscription_to_links(payload_text)
