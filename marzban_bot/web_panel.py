from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, quote_plus, unquote, urlencode, urlparse
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, MessageEntity
from telegram.constants import MessageEntityType, ParseMode

from .db import AppUser, Database, Panel
from .formatting import format_bytes, format_dt, parse_epoch_seconds
from .jalali import format_jalali_date, format_jalali_datetime, format_tehran_hour
from .marzban_client import MarzbanApiError, MarzbanClient, normalize_marzban_base_url
from .reports import build_links_document, build_report_message
from .security import decrypt_text, encrypt_text, hash_password, verify_password
from .settings import Settings
from .subscription import resolve_subscription_to_links

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class UserRow:
    username: str
    status: str
    used_traffic: int
    data_limit: int | None
    expire_dt: datetime | None


def _redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url=url, status_code=303)


def _redirect_msg(url: str, msg: str) -> RedirectResponse:
    joiner = "&" if "?" in url else "?"
    return _redirect(f"{url}{joiner}msg={quote_plus(msg)}")


def _clean_msg(text: str, *, limit: int = 220) -> str:
    cleaned = " ".join((text or "").split())
    if len(cleaned) > limit:
        return cleaned[:limit]
    return cleaned


def _normalize_template_text(text: str) -> str:
    return (text or "").replace("\r\n", "\n").replace("\r", "\n").strip()


_LEGACY_SCHEDULE_MESSAGE_TEMPLATES: frozenset[str] = frozenset(
    {
        _normalize_template_text(
            """✅ ساب ریست شد

👤 یوزر: {{username}}
📡 اینباند: {{inbound_name}}
📅 تاریخ شمسی: {{date_jalali}}
📅 تاریخ میلادی: {{date_gregorian}}
📊 باقی‌مانده: {{traffic_remaining_human}}
⏱ ریست بعدی: {{next_reset_at}}
{{links}}"""
        ),
        _normalize_template_text(
            """✅ ساب ریست شد

👤 یوزر: {{username}}
📡 اینباند: {{inbound_name}}
📅 تاریخ شمسی: {{date_jalali}}
📅 تاریخ میلادی: {{date_gregorian}}
📊 باقی‌مانده: {{traffic_remaining_human}}
⏱ ریست بعدی: {{next_reset_at}}

کانفیگ‌ها ({{configs_count}}):
{{configs}}"""
        ),
        _normalize_template_text(
            """✅ ساب ریست شد

👤 یوزر: {{username}}
📡 اینباند: {{inbound_name}}
📅 تاریخ شمسی: {{date_jalali}}
📅 تاریخ میلادی: {{date_gregorian}}
📊 باقی‌مانده: {{traffic_remaining_human}}
⏱ ریست بعدی: {{next_reset_at}}

کانفیگ‌ها ({{links_count}}):
{{links}}"""
        ),
    }
)


async def _migrate_legacy_schedule_message_templates(db: Database) -> None:
    cur = await db.conn.execute(
        """
        SELECT panel_id, username
        FROM schedule_configs
        WHERE message_template IS NOT NULL
        """
    )
    rows = await cur.fetchall()
    for row in rows:
        panel_id = int(row["panel_id"])
        username = str(row["username"])
        cfg = await db.get_schedule_config(username=username, panel_id=panel_id)
        if cfg is None or not cfg.message_template:
            continue
        if _normalize_template_text(cfg.message_template) not in _LEGACY_SCHEDULE_MESSAGE_TEMPLATES:
            continue
        await db.set_schedule_config(
            username=username,
            panel_id=panel_id,
            message_template=DEFAULT_SCHEDULE_MESSAGE_TEMPLATE,
            selected_link_keys=cfg.selected_link_keys or None,
            button_templates=cfg.button_templates or None,
        )


def _describe_panel_login_error(exc: Exception) -> str:
    if isinstance(exc, MarzbanApiError):
        return f"Marzban error: {_clean_msg(str(exc))}"

    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        if status == 401:
            return "Unauthorized (401): admin username/password اشتباه است."
        if status == 404:
            return "Not found (404): Base URL را ریشه پنل بزنید (نه /docs یا /api)."
        return f"HTTP {status}: {_clean_msg(exc.response.text)}"

    if isinstance(exc, httpx.TimeoutException):
        return "Timeout: پنل پاسخ نداد. آدرس/پورت را چک کنید."

    if isinstance(exc, httpx.InvalidURL):
        return "Invalid URL: آدرس باید با http:// یا https:// شروع شود."

    if isinstance(exc, httpx.TransportError):
        text = _clean_msg(str(exc))
        lowered = text.lower()
        if "certificate" in lowered or "ssl" in lowered:
            return "SSL error: Verify SSL را off کنید یا به جای https از http استفاده کنید."
        return f"Connection error: {text}"

    return f"Marzban login failed: {_clean_msg(str(exc))}"


def _parse_int_set(raw: str) -> set[int]:
    out: set[int] = set()
    for part in (raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.add(int(part))
        except ValueError:
            continue
    return out


def _parse_bool(raw: str | None) -> bool:
    return (raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


DEFAULT_SCHEDULE_MESSAGE_TEMPLATE = """✅ *ساب ریست شد*
━━━━━━━━━━━━━━

👤 *یوزر:* `{{username}}`
📡 *اینباند:* `{{inbound_name}}`
📅 *تاریخ شمسی:* `{{date_jalali}}`
📅 *تاریخ میلادی:* `{{date_gregorian}}`
📊 *باقی‌مانده:* `{{traffic_remaining_human}}`
⏱ *ریست بعدی:* `{{next_reset_at}}`

🔗 *کانفیگ‌ها ({{links_count}}):*
{{links}}
"""

DEFAULT_SCHEDULE_BUTTON_TEMPLATES: list[str] = [
    "📅 تاریخ شمسی: {{date_jalali}}",
    "📊 باقی‌مانده: {{traffic_remaining_human}}",
    "⏱ ریست بعدی: {{next_reset_at}}",
]

TEMPLATE_VARS = [
    ("username", "نام کاربری"),
    ("panel_name", "نام پنل"),
    ("inbound_name", "اسم اینباند (از انتخاب شما/یا پنل)"),
    ("date_jalali", "تاریخ امروز شمسی (YYYY-MM-DD)"),
    ("date_gregorian", "تاریخ امروز میلادی (YYYY-MM-DD)"),
    ("traffic_used_human", "حجم مصرف‌شده"),
    ("traffic_limit_human", "حجم کل"),
    ("traffic_remaining_human", "حجم باقی‌مانده"),
    ("next_reset_at", "ساعت ریست بعدی (تهران، با دقیقه)"),
    ("next_reset_at_jalali", "تاریخ ریست بعدی شمسی (با ساعت)"),
    ("configs", "کانفیگ‌های انتخاب‌شده (هر کانفیگ در یک خط)"),
    ("configs_count", "تعداد کانفیگ‌های انتخاب‌شده"),
    ("links", "Alias of configs (compat)"),
    ("links_count", "Alias of configs_count (compat)"),
]

_TEMPLATE_RE = re.compile(r"{{\s*([a-zA-Z0-9_]+)\s*}}")
_PERSIAN_DIGITS = str.maketrans("0123456789", "۰۱۲۳۴۵۶۷۸۹")


def _format_interval_minutes(interval_minutes: int | None) -> str:
    if interval_minutes is None:
        return "-"
    total = max(0, int(interval_minutes))
    hours = total // 60
    minutes = total % 60
    return f"{hours}.{minutes:02d}".translate(_PERSIAN_DIGITS)


def _parse_interval_minutes(raw: str) -> int | None:
    text = (raw or "").strip()
    if not text:
        return None

    delim = ":" if ":" in text else "." if "." in text else None
    if delim is None:
        try:
            hours = int(text)
        except ValueError:
            return None
        if hours <= 0:
            return None
        return hours * 60

    parts = [p.strip() for p in text.split(delim, 1)]
    if len(parts) != 2:
        return None
    try:
        hours = int(parts[0] or "0")
        minutes = int(parts[1] or "0")
    except ValueError:
        return None
    if hours < 0 or minutes < 0 or minutes >= 60:
        return None
    total = hours * 60 + minutes
    if total <= 0:
        return None
    return total


def _render_message_template(template: str, ctx: dict[str, Any]) -> str:
    def repl(match: re.Match[str]) -> str:
        key = match.group(1)
        value = ctx.get(key)
        if value is None:
            return ""
        return str(value)

    return _TEMPLATE_RE.sub(repl, template or "")


def _chunk_text(text: str, *, max_len: int = 3800) -> list[str]:
    raw = text or ""
    if len(raw) <= max_len:
        return [raw]

    parts: list[str] = []
    buf = ""
    for line in raw.splitlines(True):
        if len(buf) + len(line) > max_len and buf:
            parts.append(buf.rstrip("\n"))
            buf = ""

        if len(line) > max_len:
            remaining = line
            while len(remaining) > max_len:
                parts.append(remaining[:max_len])
                remaining = remaining[max_len:]
            buf += remaining
            continue

        buf += line

    if buf:
        parts.append(buf.rstrip("\n"))
    return [p for p in parts if p.strip()]


def _utf16_code_units(text: str) -> int:
    return len((text or "").encode("utf-16-le")) // 2


def _strip_basic_markdown(text: str) -> str:
    raw = text or ""
    raw = raw.replace("```", "")
    raw = raw.replace("`", "")
    raw = raw.replace("*", "")
    return raw


async def _expire_telegram_messages(
    bot: Bot,
    *,
    chat_id: int,
    message_ids: list[int],
    message_texts: list[str],
) -> None:
    for idx, message_id in enumerate(message_ids):
        src = message_texts[idx] if idx < len(message_texts) else ""
        plain = _strip_basic_markdown(src).strip()
        if not plain:
            plain = "منقضی شد"
        entities = [
            MessageEntity(
                type=MessageEntityType.STRIKETHROUGH,
                offset=0,
                length=_utf16_code_units(plain),
            )
        ]
        try:
            await bot.edit_message_text(
                chat_id=int(chat_id),
                message_id=int(message_id),
                text=plain,
                entities=entities,
            )
            try:
                await bot.edit_message_reply_markup(chat_id=int(chat_id), message_id=int(message_id), reply_markup=None)
            except Exception:
                pass
        except Exception:
            pass


def _compact_one_line(text: str) -> str:
    return " ".join((text or "").split())


def _truncate_telegram_button_text(text: str, *, max_len: int = 64) -> str:
    value = _compact_one_line(text).replace("`", "")
    if len(value) <= max_len:
        return value
    if max_len <= 1:
        return value[:max_len]
    return value[: max_len - 1].rstrip() + "…"


def _build_telegram_info_buttons(
    templates: list[str] | None,
    ctx: dict[str, Any],
) -> InlineKeyboardMarkup | None:
    templates = list(templates or [])
    if not templates:
        templates = list(DEFAULT_SCHEDULE_BUTTON_TEMPLATES)

    rows: list[list[InlineKeyboardButton]] = []
    for idx, tpl in enumerate(templates):
        rendered = _render_message_template(str(tpl or ""), ctx).strip()
        rendered = _truncate_telegram_button_text(rendered, max_len=64)
        if not rendered:
            continue
        rows.append([InlineKeyboardButton(text=rendered, callback_data=f"info:{idx}")])

    if not rows:
        return None
    return InlineKeyboardMarkup(rows)


async def _send_telegram_text(
    bot: Bot,
    *,
    chat_id: int,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> list[Any]:
    parts = _chunk_text(text)
    return await _send_telegram_parts(
        bot,
        chat_id=chat_id,
        parts=parts,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN,
    )


async def _send_telegram_parts(
    bot: Bot,
    *,
    chat_id: int,
    parts: list[str],
    reply_markup: InlineKeyboardMarkup | None = None,
    parse_mode: str | None = ParseMode.MARKDOWN,
) -> list[Any]:
    last_idx = max(0, len(parts) - 1)
    out: list[Any] = []
    for idx, part in enumerate(parts):
        msg = await bot.send_message(
            chat_id=chat_id,
            text=part,
            parse_mode=parse_mode,
            reply_markup=(reply_markup if idx == last_idx else None),
        )
        out.append(msg)
    return out


def _format_links_markdown(links: list[str]) -> str:
    blocks: list[str] = []
    idx = 0
    for link in links:
        raw = str(link or "").strip()
        if not raw:
            continue
        raw = raw.replace("`", "")
        idx += 1
        num = str(idx).translate(_PERSIAN_DIGITS)
        scheme = _link_scheme(raw)
        proto = scheme.upper() if scheme and scheme != "unknown" else ""
        header = f"👇 *کانفیگ {num}*"
        if proto:
            header = f"{header} ({proto})"
        blocks.append(header)
        blocks.append(f"`{raw}`")
        blocks.append("")
    return "\n".join(blocks).rstrip()


def _get_tz(tz_name: str):
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return UTC


def _link_scheme(link: str) -> str:
    text = (link or "").strip()
    if "://" in text:
        return text.split("://", 1)[0].lower()
    return "unknown"


def _fingerprint(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()[:12]


def _decode_vmess_json(link: str) -> dict[str, Any] | None:
    raw = (link or "").strip()
    if not raw.lower().startswith("vmess://"):
        return None
    b64 = raw[8:].strip()
    try:
        padded = b64 + ("=" * (-len(b64) % 4))
        decoded = base64.b64decode(padded)
        obj = json.loads(decoded.decode("utf-8", errors="strict"))
        if not isinstance(obj, dict):
            return None
        return obj
    except (binascii.Error, ValueError, UnicodeError, json.JSONDecodeError):
        return None


def _vmess_label_key_legacy(link: str) -> tuple[str, str, str, str]:
    obj = _decode_vmess_json(link)
    ps = str((obj or {}).get("ps") or "").strip()
    add = str((obj or {}).get("add") or "").strip()
    port = str((obj or {}).get("port") or "").strip()
    hostport = f"{add}:{port}".strip(":") if (add or port) else ""

    legacy_label = ps or hostport or "vmess"
    label = f"{ps} · {hostport}" if (ps and hostport) else legacy_label

    if obj is not None:
        stable = {
            "ps": ps,
            "add": add,
            "port": port,
            "net": str(obj.get("net") or "").strip(),
            "type": str(obj.get("type") or "").strip(),
            "tls": str(obj.get("tls") or "").strip(),
            "sni": str(obj.get("sni") or "").strip(),
            "host": str(obj.get("host") or "").strip(),
            "path": str(obj.get("path") or "").strip(),
            "alpn": str(obj.get("alpn") or "").strip(),
        }
        key_raw = json.dumps(stable, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        key = f"vmess:{_fingerprint(key_raw)}"
    else:
        key = f"vmess:{_fingerprint(link)}"

    legacy_key = f"vmess:{legacy_label}"
    return (label, key, legacy_key, key)


_STABLE_URL_QUERY_KEYS: frozenset[str] = frozenset(
    {
        # Common xray transport differentiators (stable)
        "type",
        "security",
        "path",
        "serviceName",
        "mode",
        "headerType",
        "host",
        "authority",
        # Misc (keep stable + low-variance)
        "encryption",
        "flow",
    }
)


def _url_label_key_legacy(link: str, scheme: str) -> tuple[str, str, str, str]:
    raw = (link or "").strip()
    try:
        parsed = urlparse(raw)
    except Exception:
        label = raw[:32] or "link"
        key = f"{scheme}:{_fingerprint(raw)}"
        legacy_key = f"{scheme}:{label}"
        return (label, key, legacy_key, key)

    hostport = parsed.netloc.split("@", 1)[-1] if parsed.netloc else ""
    frag = unquote(parsed.fragment or "").strip()

    legacy_label = frag or hostport or scheme
    label = f"{frag} · {hostport}" if (frag and hostport) else legacy_label

    host = parsed.hostname or ""
    port = "" if parsed.port is None else str(int(parsed.port))
    path = parsed.path or ""

    query_items = parse_qsl(parsed.query, keep_blank_values=True)

    # Back-compat key (includes full query). This can change when subscription payload shuffles
    # parameters (ex: `sni` swapping), so it must NOT be used for selection persistence.
    compat_query = urlencode(sorted(query_items), doseq=True)
    compat_raw = f"{scheme}|{host}|{port}|{path}|{compat_query}|{frag}"
    compat_key = f"{scheme}:{_fingerprint(compat_raw)}"

    # Stable key (used for persistence): only include low-variance transport params.
    stable_items = [(k, v) for k, v in query_items if k in _STABLE_URL_QUERY_KEYS and str(v).strip() != ""]
    stable_query = urlencode(sorted(stable_items), doseq=True)
    stable_raw = f"{scheme}|{host}|{port}|{path}|{stable_query}|{frag}"
    key = f"{scheme}:{_fingerprint(stable_raw)}"
    legacy_key = f"{scheme}:{legacy_label}"
    return (label, key, legacy_key, compat_key)


def _link_label_key_legacy(link: str) -> tuple[str, str, str, str]:
    scheme = _link_scheme(link)
    if scheme == "vmess":
        return _vmess_label_key_legacy(link)
    if scheme in {"vless", "trojan", "ss", "socks"}:
        return _url_label_key_legacy(link, scheme)

    raw = (link or "").strip()
    label = raw[:32] or "link"
    key = f"{scheme}:{_fingerprint(f'{scheme}|{raw}')}"
    legacy_key = f"{scheme}:{label}"
    return (label, key, legacy_key, key)


def _build_link_items(links: list[str]) -> tuple[dict[str, list[dict[str, str]]], list[dict[str, str]]]:
    groups: dict[str, list[dict[str, str]]] = {}
    items: list[dict[str, str]] = []
    seen: set[str] = set()
    for link in links:
        link = (link or "").strip()
        if not link or link in seen:
            continue
        seen.add(link)
        scheme = _link_scheme(link)
        label, key, legacy_key, compat_key = _link_label_key_legacy(link)
        item = {
            "scheme": scheme,
            "label": label,
            "key": key,
            "compat_key": compat_key,
            "legacy_key": legacy_key,
            "url": link,
        }
        items.append(item)
        groups.setdefault(scheme, []).append(item)
    return groups, items


def _migrate_selected_link_keys_to_stable(selected: list[str], link_items: list[dict[str, str]]) -> list[str]:
    keys = [str(k).strip() for k in (selected or []) if str(k).strip()]
    if not keys:
        return []

    stable_keys = {str(it.get("key")) for it in link_items if str(it.get("key") or "").strip()}

    compat_to_stable: dict[str, str] = {}
    for it in link_items:
        compat = str(it.get("compat_key") or "").strip()
        stable = str(it.get("key") or "").strip()
        if compat and stable and compat not in compat_to_stable:
            compat_to_stable[compat] = stable

    legacy_counts: dict[str, int] = {}
    legacy_last: dict[str, str] = {}
    for it in link_items:
        legacy = str(it.get("legacy_key") or "").strip()
        stable = str(it.get("key") or "").strip()
        if not legacy or not stable:
            continue
        legacy_counts[legacy] = legacy_counts.get(legacy, 0) + 1
        legacy_last[legacy] = stable
    legacy_to_stable = {k: v for k, v in legacy_last.items() if legacy_counts.get(k, 0) == 1}

    out: list[str] = []
    seen: set[str] = set()
    for k in keys:
        stable = None
        if k in stable_keys:
            stable = k
        elif k in compat_to_stable:
            stable = compat_to_stable[k]
        elif k in legacy_to_stable:
            stable = legacy_to_stable[k]
        if not stable or stable in seen:
            continue
        seen.add(stable)
        out.append(stable)
    return out


def _extract_inbound_names(user: dict[str, Any]) -> list[str]:
    raw = user.get("inbounds")
    if not isinstance(raw, dict):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for tags in raw.values():
        if not isinstance(tags, list):
            continue
        for tag in tags:
            name = str(tag).strip()
            if not name or name in seen:
                continue
            seen.add(name)
            out.append(name)
    return out


async def _get_db(request: Request) -> Database:
    db = getattr(request.app.state, "db", None)
    if not isinstance(db, Database):
        raise RuntimeError("DB not initialized")
    return db


async def _current_user(request: Request) -> AppUser | None:
    user_id = request.session.get("user_id")
    if not user_id:
        return None
    db = await _get_db(request)
    return await db.get_user_by_id(int(user_id))


async def _require_user(request: Request) -> AppUser:
    user = await _current_user(request)
    if user is None:
        raise HTTPException(status_code=303, headers={"Location": "/login"})
    return user


async def _user_panels(db: Database, user: AppUser) -> list[Panel]:
    return await db.list_panels(owner_user_id=user.id)


async def _get_active_panel(request: Request, user: AppUser) -> Panel | None:
    db = await _get_db(request)
    panels = await _user_panels(db, user)
    if not panels:
        return None
    active = request.session.get("active_panel_id")
    if active:
        panel = await db.get_panel(owner_user_id=user.id, panel_id=int(active))
        if panel is not None:
            return panel
    panel = panels[0]
    request.session["active_panel_id"] = panel.id
    return panel


async def _get_panel_client_by_app(app: FastAPI, panel: Panel) -> MarzbanClient:
    settings: Settings = app.state.settings
    clients: dict[int, MarzbanClient] = app.state.panel_clients
    locks: dict[int, asyncio.Lock] = app.state.panel_client_locks
    lock = locks.setdefault(panel.id, asyncio.Lock())
    async with lock:
        existing = clients.get(panel.id)
        if existing is not None:
            return existing
        password = decrypt_text(settings.app_secret_key, panel.admin_password_enc)
        client = MarzbanClient(
            base_url=panel.base_url,
            username=panel.admin_username,
            password=password,
            verify_ssl=panel.verify_ssl,
        )
        await client.login()
        clients[panel.id] = client
        return client


async def _get_panel_client(request: Request, panel: Panel) -> MarzbanClient:
    return await _get_panel_client_by_app(request.app, panel)


async def _invalidate_panel_client_by_app(app: FastAPI, panel_id: int) -> None:
    clients: dict[int, MarzbanClient] = app.state.panel_clients
    client = clients.pop(int(panel_id), None)
    if client is not None:
        await client.aclose()


async def _invalidate_panel_client(request: Request, panel_id: int) -> None:
    await _invalidate_panel_client_by_app(request.app, panel_id)


async def _fetch_subscription_payload_by_app(app: FastAPI, url: str, *, verify_ssl: bool) -> str | None:
    url = (url or "").strip()
    if not url:
        return None
    http: httpx.AsyncClient = app.state.http
    try:
        resp = await http.get(url, verify=verify_ssl)
        resp.raise_for_status()
        return resp.text
    except Exception:
        return None


async def _fetch_subscription_payload(request: Request, url: str, *, verify_ssl: bool) -> str | None:
    return await _fetch_subscription_payload_by_app(request.app, url, verify_ssl=verify_ssl)


async def _resolve_links_by_app(app: FastAPI, panel: Panel, user: dict[str, Any]) -> list[str]:
    links_raw = user.get("links")
    api_links: list[str] = []
    if isinstance(links_raw, list):
        api_links = [str(x).strip() for x in links_raw if str(x).strip()]

    payload = await _fetch_subscription_payload_by_app(
        app,
        str(user.get("subscription_url", "")),
        verify_ssl=panel.verify_ssl,
    )
    if payload:
        resolved = resolve_subscription_to_links(payload)
        if resolved and not (len(resolved) == 1 and "://" not in resolved[0]):
            return resolved
    return api_links


async def _resolve_links(request: Request, panel: Panel, user: dict[str, Any]) -> list[str]:
    return await _resolve_links_by_app(request.app, panel, user)


async def _scheduler_tick(app: FastAPI) -> None:
    db: Database = app.state.db
    settings: Settings = app.state.settings
    cfg = await db.get_telegram_config()
    if not cfg.bot_token:
        return

    bot = Bot(token=cfg.bot_token)
    now_epoch = int(time.time())
    due = await db.get_due_schedules(now=now_epoch)

    for sched in due:
        try:
            panel = await db.get_panel_by_id(int(sched.panel_id))
            if panel is None:
                await db.mark_schedule_result(
                    username=sched.username,
                    panel_id=int(sched.panel_id),
                    next_run_at=now_epoch + 3600,
                    last_run_at=now_epoch,
                    last_error="Panel not found",
                )
                continue

            binding = await db.get_binding(username=sched.username, panel_id=panel.id)
            chat_id = binding.chat_id if binding else panel.default_chat_id
            if chat_id is None:
                await db.mark_schedule_result(
                    username=sched.username,
                    panel_id=panel.id,
                    next_run_at=now_epoch + 300,
                    last_run_at=now_epoch,
                    last_error="No chat_id (set panel default_chat_id or bind user)",
                )
                continue
            prev_msg_state = await db.get_schedule_message_state(username=sched.username, panel_id=panel.id)

            client = await _get_panel_client_by_app(app, panel)
            sched_cfg = await db.get_schedule_config(username=sched.username, panel_id=panel.id)
            selected_keys = sched_cfg.selected_link_keys if sched_cfg is not None else []
            message_template = sched_cfg.message_template if sched_cfg is not None else None
            button_templates = sched_cfg.button_templates if sched_cfg is not None else None

            # Best-effort migration: if the stored selection used the old (unstable) keying,
            # rewrite it to the new stable keys before we revoke (since revoke_sub can change
            # parts of the URL query like `sni`, causing old hashes to stop matching).
            if selected_keys:
                try:
                    pre_user = await client.get_user(sched.username)
                    pre_links = await _resolve_links_by_app(app, panel, pre_user)
                    _pre_groups, pre_items = _build_link_items(pre_links)
                    migrated = _migrate_selected_link_keys_to_stable(selected_keys, pre_items)
                    if migrated and migrated != selected_keys:
                        await db.set_schedule_config(
                            username=sched.username,
                            panel_id=panel.id,
                            message_template=message_template,
                            selected_link_keys=migrated,
                            button_templates=button_templates,
                        )
                        selected_keys = migrated
                except Exception:
                    logger.exception(
                        "failed migrating schedule config keys (username=%s, panel_id=%s)",
                        sched.username,
                        panel.id,
                    )

            user = await client.revoke_user_subscription(sched.username)
            usage = await client.get_user_usage(sched.username)

            interval_seconds = max(60, int(sched.interval_minutes) * 60)
            next_run_at = int(time.time()) + interval_seconds
            now_dt = datetime.now(tz=UTC)
            next_dt = datetime.fromtimestamp(next_run_at, tz=UTC)

            links_all = await _resolve_links_by_app(app, panel, user)
            _groups, link_items = _build_link_items(links_all)
            selected_set = set(selected_keys)

            links_selected = links_all
            if selected_set:
                filtered = [
                    it["url"]
                    for it in link_items
                    if it.get("key") in selected_set
                    or it.get("compat_key") in selected_set
                    or it.get("legacy_key") in selected_set
                ]
                if filtered:
                    links_selected = filtered

            inbound_names = _extract_inbound_names(user)
            inbound_name = ", ".join(inbound_names) if inbound_names else "-"

            used = int(user.get("used_traffic") or 0)
            data_limit_raw = user.get("data_limit")
            data_limit = None
            try:
                if data_limit_raw not in (None, "", 0, "0"):
                    data_limit = int(data_limit_raw)
            except Exception:
                data_limit = None
            if data_limit is not None and data_limit <= 0:
                data_limit = None
            remaining = None if data_limit is None else max(0, int(data_limit) - int(used))

            now_local = now_dt.astimezone(_get_tz(settings.timezone))
            date_gregorian = now_local.strftime("%Y-%m-%d")

            ctx = {
                "panel_name": panel.name,
                "username": str(user.get("username") or sched.username),
                "inbound_name": inbound_name,
                "date_jalali": format_jalali_date(now_dt, settings.timezone),
                "date_gregorian": date_gregorian,
                "traffic_used_human": format_bytes(used),
                "traffic_limit_human": format_bytes(data_limit),
                "traffic_remaining_human": format_bytes(remaining),
                "next_reset_at": format_tehran_hour(next_dt, "Asia/Tehran"),
                "next_reset_at_jalali": format_jalali_datetime(next_dt, settings.timezone),
                "configs": _format_links_markdown(links_selected),
                "configs_count": len(links_selected),
                "links": _format_links_markdown(links_selected),
                "links_count": len(links_selected),
            }

            template = (message_template or DEFAULT_SCHEDULE_MESSAGE_TEMPLATE)
            message = _render_message_template(template, ctx).strip() or build_report_message(
                user=user,
                usage=usage,
                tz_name=settings.timezone,
                now=now_dt,
                next_reset_at=next_dt,
                interval_hours=None,
                reason=f"scheduled revoke_sub (panel: {panel.name})",
            )

            reply_markup = _build_telegram_info_buttons(button_templates, ctx)
            parts = _chunk_text(message)
            sent_messages = await _send_telegram_parts(
                bot,
                chat_id=int(chat_id),
                parts=parts,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN,
            )
            message_ids = [int(getattr(m, "message_id")) for m in sent_messages if getattr(m, "message_id", None) is not None]
            if message_ids:
                await db.set_schedule_message_state(
                    username=sched.username,
                    panel_id=panel.id,
                    chat_id=int(chat_id),
                    message_ids=message_ids,
                    message_texts=parts,
                )
                if (
                    prev_msg_state is not None
                    and prev_msg_state.message_ids
                    and (prev_msg_state.chat_id != int(chat_id) or prev_msg_state.message_ids != message_ids)
                ):
                    await _expire_telegram_messages(
                        bot,
                        chat_id=int(prev_msg_state.chat_id),
                        message_ids=list(prev_msg_state.message_ids),
                        message_texts=list(prev_msg_state.message_texts),
                    )

            await db.mark_schedule_result(
                username=sched.username,
                panel_id=panel.id,
                next_run_at=next_run_at,
                last_run_at=int(time.time()),
                last_error=None,
            )
        except Exception as e:
            await db.mark_schedule_result(
                username=sched.username,
                panel_id=int(sched.panel_id),
                next_run_at=int(time.time()) + 300,
                last_run_at=int(time.time()),
                last_error=str(e)[:500],
            )


async def _scheduler_loop(app: FastAPI) -> None:
    while True:
        try:
            await _scheduler_tick(app)
        except Exception:
            logger.exception("scheduler tick failed")
        await asyncio.sleep(max(5, int(app.state.settings.poll_interval_seconds)))


async def _try_send_telegram_message(app: FastAPI, *, chat_id: int, text: str) -> None:
    db: Database = app.state.db
    cfg = await db.get_telegram_config()
    if not cfg.bot_token:
        return
    try:
        bot = Bot(token=cfg.bot_token)
        await _send_telegram_text(bot, chat_id=chat_id, text=text)
    except Exception:
        logger.exception("telegram send_message failed")


async def _telegram_handle_command(
    *,
    db: Database,
    bot: Bot,
    cfg: Any,
    text: str,
    chat_id: int,
    user_id: int,
) -> None:
    admins = _parse_int_set(getattr(cfg, "admin_user_ids", "") or "")

    if text.startswith("/whoami"):
        await bot.send_message(chat_id=chat_id, text=f"user_id={user_id}\nchat_id={chat_id}")
        return

    if text.startswith("/start"):
        parts = text.split()
        if len(parts) >= 2 and int(user_id) in admins:
            try:
                panel_id = int(parts[1])
            except ValueError:
                await bot.send_message(chat_id=chat_id, text="Usage: /start <panel_id>")
                return
            panel = await db.get_panel_by_id(panel_id)
            if panel is None:
                await bot.send_message(chat_id=chat_id, text="Panel not found")
                return
            await db.update_panel(
                owner_user_id=panel.owner_user_id,
                panel_id=panel.id,
                name=panel.name,
                base_url=panel.base_url,
                admin_username=panel.admin_username,
                admin_password_enc=panel.admin_password_enc,
                verify_ssl=panel.verify_ssl,
                default_chat_id=int(chat_id),
            )
            await bot.send_message(chat_id=chat_id, text=f"default_chat_id saved for panel {panel.id} ({panel.name})")
            return

        await bot.send_message(chat_id=chat_id, text="Use /whoami to get ids.")
        return


async def _telegram_handle_update(*, db: Database, cfg: Any, bot: Bot, update: dict[str, Any]) -> bool:
    cbq = update.get("callback_query") or {}
    if isinstance(cbq, dict) and cbq.get("id"):
        try:
            await bot.answer_callback_query(callback_query_id=str(cbq.get("id")))
        except Exception:
            pass
        return True

    msg = (update.get("message") or {})
    text = (msg.get("text") or "").strip()
    chat = msg.get("chat") or {}
    from_user = msg.get("from") or {}
    chat_id = chat.get("id")
    user_id = from_user.get("id")

    if not (text.startswith("/") and chat_id and user_id):
        return False

    await _telegram_handle_command(
        db=db,
        bot=bot,
        cfg=cfg,
        text=text,
        chat_id=int(chat_id),
        user_id=int(user_id),
    )
    return True


async def _telegram_poll_tick(app: FastAPI) -> int:
    db: Database = app.state.db
    cfg = await db.get_telegram_config()
    if not cfg.bot_token:
        return 0

    enabled = _parse_bool(await db.get_kv("telegram_polling_enabled"))
    if not enabled:
        return 0

    lock: asyncio.Lock = app.state.telegram_poll_lock
    async with lock:
        bot = Bot(token=cfg.bot_token)

        offset_raw = (await db.get_kv("telegram_polling_offset") or "").strip()
        offset = None
        if offset_raw and offset_raw.isdigit():
            offset = int(offset_raw)

        try:
            updates = await bot.get_updates(offset=offset, timeout=20, allowed_updates=["message", "callback_query"])
        except Exception as e:
            await db.set_kv("telegram_polling_last_error", f"{type(e).__name__}: {str(e)[:200]}")
            return 0

        processed = 0
        max_update_id: int | None = None

        for upd in updates:
            try:
                update_dict = upd.to_dict() if hasattr(upd, "to_dict") else {}
                if await _telegram_handle_update(db=db, cfg=cfg, bot=bot, update=update_dict):
                    processed += 1
                update_id = getattr(upd, "update_id", None)
                if isinstance(update_id, int):
                    max_update_id = update_id if max_update_id is None else max(max_update_id, update_id)
            except Exception:
                logger.exception("telegram update handling failed")

        if max_update_id is not None:
            await db.set_kv("telegram_polling_offset", str(int(max_update_id) + 1))

        if processed:
            await db.set_kv("telegram_polling_last_error", "")

        return processed


async def _telegram_poll_loop(app: FastAPI) -> None:
    while True:
        try:
            processed = await _telegram_poll_tick(app)
        except Exception:
            logger.exception("telegram poll tick failed")
            await asyncio.sleep(2)
            continue

        await asyncio.sleep(0.2 if processed else 1.0)


def create_app(settings: Settings) -> FastAPI:
    app = FastAPI(title="Marzban Bot Panel")
    app.state.settings = settings
    app.add_middleware(SessionMiddleware, secret_key=settings.app_secret_key, same_site="lax")

    base_dir = Path(__file__).resolve().parent
    templates_dir = base_dir / "web" / "templates"
    static_dir = base_dir / "web" / "static"
    templates = Jinja2Templates(directory=str(templates_dir))
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    app.state.panel_clients = {}
    app.state.panel_client_locks = {}

    @app.on_event("startup")
    async def _startup() -> None:
        db = Database(settings.db_path)
        await db.connect()
        await _migrate_legacy_schedule_message_templates(db)
        app.state.db = db

        app.state.http = httpx.AsyncClient(timeout=httpx.Timeout(20.0))
        app.state.scheduler_task = asyncio.create_task(_scheduler_loop(app))
        app.state.telegram_poll_lock = asyncio.Lock()
        app.state.telegram_poll_task = asyncio.create_task(_telegram_poll_loop(app))

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        task = getattr(app.state, "scheduler_task", None)
        if isinstance(task, asyncio.Task):
            task.cancel()
            try:
                await task
            except Exception:
                pass
        task = getattr(app.state, "telegram_poll_task", None)
        if isinstance(task, asyncio.Task):
            task.cancel()
            try:
                await task
            except Exception:
                pass

        http = getattr(app.state, "http", None)
        if isinstance(http, httpx.AsyncClient):
            await http.aclose()

        clients: dict[int, MarzbanClient] = app.state.panel_clients
        for client in list(clients.values()):
            try:
                await client.aclose()
            except Exception:
                pass
        app.state.panel_clients = {}

        db = getattr(app.state, "db", None)
        if isinstance(db, Database):
            await db.aclose()

    @app.get("/", response_class=HTMLResponse)
    async def root(request: Request) -> RedirectResponse:
        user = await _current_user(request)
        if user is None:
            return _redirect("/login")
        return _redirect("/users")

    @app.get("/health")
    async def health() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/login", response_class=HTMLResponse)
    async def login_get(request: Request, msg: str = "") -> HTMLResponse:
        return templates.TemplateResponse(request=request, name="login.html", context={"msg": msg})

    @app.post("/login")
    async def login_post(request: Request, username: str = Form(...), password: str = Form(...)) -> RedirectResponse:
        db = await _get_db(request)
        user = await db.get_user_by_username(username.strip())
        if user is None or not verify_password(password, user.password_hash):
            return _redirect("/login?msg=Invalid+credentials")
        request.session["user_id"] = user.id
        return _redirect("/panels")

    @app.get("/logout")
    async def logout(request: Request) -> RedirectResponse:
        request.session.clear()
        return _redirect("/login?msg=Logged+out")

    @app.get("/signup", response_class=HTMLResponse)
    async def signup_get(request: Request, msg: str = "") -> HTMLResponse:
        db = await _get_db(request)
        users_count = await db.count_users()
        return templates.TemplateResponse(
            request=request,
            name="signup.html",
            context={
                "msg": msg,
                "needs_code": bool(settings.signup_code),
                "open_signup": users_count == 0 and not settings.signup_code,
            },
        )

    @app.post("/signup")
    async def signup_post(
        request: Request,
        username: str = Form(...),
        password: str = Form(...),
        signup_code: str = Form(""),
    ) -> RedirectResponse:
        db = await _get_db(request)
        username = username.strip()
        if not username:
            return _redirect("/signup?msg=Username+required")

        users_count = await db.count_users()
        if settings.signup_code:
            if signup_code.strip() != settings.signup_code:
                return _redirect("/signup?msg=Invalid+signup+code")
        elif users_count != 0:
            return _redirect("/login?msg=Signup+disabled")

        if await db.get_user_by_username(username):
            return _redirect("/signup?msg=Username+exists")
        user = await db.create_user(username=username, password_hash=hash_password(password))
        request.session["user_id"] = user.id
        return _redirect("/panels")

    @app.get("/panels", response_class=HTMLResponse)
    async def panels_page(request: Request, msg: str = "") -> HTMLResponse:
        user = await _require_user(request)
        db = await _get_db(request)
        panels = await db.list_panels(owner_user_id=user.id)
        active = request.session.get("active_panel_id")
        return templates.TemplateResponse(
            request=request,
            name="panels.html",
            context={"user": user, "panels": panels, "active_panel_id": active, "msg": msg},
        )

    @app.post("/panels/new")
    async def panels_new(
        request: Request,
        name: str = Form(...),
        base_url: str = Form(...),
        admin_username: str = Form(...),
        admin_password: str = Form(...),
        verify_ssl: str = Form("1"),
    ) -> RedirectResponse:
        user = await _require_user(request)
        db = await _get_db(request)
        raw_base_url = (base_url or "").strip()
        if not raw_base_url:
            return _redirect_msg("/panels", "Base URL required")
        base_url = normalize_marzban_base_url(raw_base_url)
        if not base_url:
            return _redirect_msg("/panels", "Invalid base URL. Example: https://domain یا https://domain/marzban")
        verify = verify_ssl.strip() in {"1", "true", "yes", "on"}

        try:
            client = MarzbanClient(
                base_url=base_url,
                username=admin_username.strip(),
                password=admin_password,
                verify_ssl=verify,
            )
        except ValueError:
            return _redirect_msg("/panels", "Invalid base URL. Example: https://domain یا https://domain/marzban")
        try:
            await client.login()
        except Exception as e:
            return _redirect_msg("/panels", _describe_panel_login_error(e))
        finally:
            await client.aclose()

        enc = encrypt_text(settings.app_secret_key, admin_password)
        panel = await db.create_panel(
            owner_user_id=user.id,
            name=name.strip() or base_url,
            base_url=base_url,
            admin_username=admin_username.strip(),
            admin_password_enc=enc,
            verify_ssl=verify,
        )
        request.session["active_panel_id"] = panel.id
        await db.migrate_legacy_data(default_panel_id=panel.id)
        return _redirect("/users")

    @app.get("/panels/select/{panel_id}")
    async def panels_select(request: Request, panel_id: int, next: str = "/users") -> RedirectResponse:
        user = await _require_user(request)
        db = await _get_db(request)
        panel = await db.get_panel(owner_user_id=user.id, panel_id=int(panel_id))
        if panel is None:
            return _redirect("/panels?msg=Panel+not+found")
        request.session["active_panel_id"] = panel.id
        if not next.startswith("/"):
            next = "/users"
        return _redirect(next)

    @app.get("/panels/{panel_id}", response_class=HTMLResponse)
    async def panel_edit_get(request: Request, panel_id: int, msg: str = "") -> HTMLResponse:
        user = await _require_user(request)
        db = await _get_db(request)
        panel = await db.get_panel(owner_user_id=user.id, panel_id=int(panel_id))
        if panel is None:
            return templates.TemplateResponse(request=request, name="error.html", context={"msg": "Panel not found"})
        return templates.TemplateResponse(
            request=request,
            name="panel_edit.html",
            context={"user": user, "panel": panel, "msg": msg},
        )

    @app.post("/panels/{panel_id}/update")
    async def panel_edit_post(
        request: Request,
        panel_id: int,
        name: str = Form(...),
        base_url: str = Form(...),
        admin_username: str = Form(...),
        admin_password: str = Form(""),
        verify_ssl: str = Form("1"),
        default_chat_id: str = Form(""),
    ) -> RedirectResponse:
        user = await _require_user(request)
        db = await _get_db(request)
        panel = await db.get_panel(owner_user_id=user.id, panel_id=int(panel_id))
        if panel is None:
            return _redirect("/panels?msg=Panel+not+found")

        base_url = normalize_marzban_base_url(base_url)
        if not base_url:
            return _redirect_msg(f"/panels/{panel_id}", "Invalid base URL. Example: https://domain یا https://domain/marzban")
        verify = verify_ssl.strip() in {"1", "true", "yes", "on"}
        chat_id = None
        if default_chat_id.strip():
            try:
                chat_id = int(default_chat_id.strip())
            except ValueError:
                return _redirect(f"/panels/{panel_id}?msg=Invalid+chat_id")

        enc_password = panel.admin_password_enc
        if admin_password.strip():
            enc_password = encrypt_text(settings.app_secret_key, admin_password.strip())

        await db.update_panel(
            owner_user_id=user.id,
            panel_id=panel.id,
            name=name.strip() or panel.name,
            base_url=base_url,
            admin_username=admin_username.strip(),
            admin_password_enc=enc_password,
            verify_ssl=verify,
            default_chat_id=chat_id,
        )
        await _invalidate_panel_client(request, panel.id)
        return _redirect(f"/panels/{panel.id}?msg=Saved")

    @app.post("/panels/{panel_id}/delete")
    async def panel_delete(request: Request, panel_id: int) -> RedirectResponse:
        user = await _require_user(request)
        db = await _get_db(request)
        await db.delete_panel(owner_user_id=user.id, panel_id=int(panel_id))
        if request.session.get("active_panel_id") == int(panel_id):
            request.session.pop("active_panel_id", None)
        await _invalidate_panel_client(request, int(panel_id))
        return _redirect("/panels?msg=Deleted")

    @app.get("/users", response_class=HTMLResponse)
    async def users_page(request: Request, search: str = "", page: int = 1, msg: str = "") -> HTMLResponse:
        user = await _require_user(request)
        db = await _get_db(request)
        panels = await _user_panels(db, user)
        panel = await _get_active_panel(request, user)
        if panel is None:
            return _redirect("/panels?msg=Add+a+panel+first")

        search = (search or "").strip()
        page = max(1, int(page or 1))
        limit = 50
        offset = (page - 1) * limit

        client = await _get_panel_client(request, panel)
        data = await client.get_users(offset=offset, limit=limit, search=search or None)
        users_raw = data.get("users") if isinstance(data, dict) else []
        total = int(data.get("total") or 0) if isinstance(data, dict) else 0

        rows: list[UserRow] = []
        if isinstance(users_raw, list):
            for u in users_raw:
                if not isinstance(u, dict):
                    continue
                expire_dt = parse_epoch_seconds(u.get("expire"))
                data_limit = u.get("data_limit")
                data_limit_int = None if data_limit in (None, 0, "0") else int(data_limit or 0)
                rows.append(
                    UserRow(
                        username=str(u.get("username", "")),
                        status=str(u.get("status", "")),
                        used_traffic=int(u.get("used_traffic") or 0),
                        data_limit=data_limit_int,
                        expire_dt=expire_dt,
                    )
                )

        pages = max(1, (total + limit - 1) // limit) if total else 1
        return templates.TemplateResponse(
            request=request,
            name="users.html",
            context={
                "user": user,
                "panels": panels,
                "panel": panel,
                "rows": rows,
                "search": search,
                "page": page,
                "pages": pages,
                "total": total,
                "msg": msg,
                "format_bytes": format_bytes,
                "format_dt": format_dt,
                "tz_name": settings.timezone,
            },
        )

    @app.get("/users/{username}", response_class=HTMLResponse)
    async def user_detail(request: Request, username: str, msg: str = "", open_cfg: int = 0) -> HTMLResponse:
        user = await _require_user(request)
        db = await _get_db(request)
        panels = await _user_panels(db, user)
        panel = await _get_active_panel(request, user)
        if panel is None:
            return _redirect("/panels?msg=Add+a+panel+first")

        client = await _get_panel_client(request, panel)
        api_user = await client.get_user(username)

        schedule = await db.get_schedule(username=username, panel_id=panel.id)
        binding = await db.get_binding(username=username, panel_id=panel.id)

        expire_dt = parse_epoch_seconds(api_user.get("expire"))
        raw_limit = api_user.get("data_limit")
        data_limit_display = None if raw_limit in (None, 0, "0") else int(raw_limit or 0)

        resolved_links = await _resolve_links(request, panel, api_user)
        link_groups, link_items = _build_link_items(resolved_links)
        vless_items = [it for it in link_items if it.get("scheme") == "vless"]

        schedule_cfg = await db.get_schedule_config(username=username, panel_id=panel.id)
        schedule_template = (schedule_cfg.message_template if schedule_cfg is not None else None) or DEFAULT_SCHEDULE_MESSAGE_TEMPLATE
        schedule_selected_keys = schedule_cfg.selected_link_keys if schedule_cfg is not None else []
        schedule_button_templates = (schedule_cfg.button_templates if schedule_cfg is not None else []) or DEFAULT_SCHEDULE_BUTTON_TEMPLATES
        if schedule_cfg is not None and schedule_selected_keys:
            migrated = _migrate_selected_link_keys_to_stable(schedule_selected_keys, link_items)
            if migrated and migrated != schedule_selected_keys:
                await db.set_schedule_config(
                    username=username,
                    panel_id=panel.id,
                    message_template=schedule_cfg.message_template,
                    selected_link_keys=migrated,
                    button_templates=schedule_cfg.button_templates,
                )
                schedule_selected_keys = migrated

        next_reset_dt = None
        if schedule and schedule.enabled:
            next_reset_dt = datetime.fromtimestamp(int(schedule.next_run_at), tz=UTC)

        return templates.TemplateResponse(
            request=request,
            name="user.html",
            context={
                "user": user,
                "panels": panels,
                "panel": panel,
                "api_user": api_user,
                "expire_dt": expire_dt,
                "data_limit_display": data_limit_display,
                "schedule": schedule,
                "binding": binding,
                "resolved_links": resolved_links,
                "link_groups": link_groups,
                "vless_items": vless_items,
                "next_reset_dt": next_reset_dt,
                "schedule_template": schedule_template,
                "schedule_selected_keys": schedule_selected_keys,
                "schedule_button_templates": schedule_button_templates,
                "template_vars": TEMPLATE_VARS,
                "open_cfg": int(open_cfg or 0),
                "msg": msg,
                "format_bytes": format_bytes,
                "format_dt": format_dt,
                "format_interval": _format_interval_minutes,
                "tz_name": settings.timezone,
            },
        )

    @app.get("/users/{username}/links.txt")
    async def user_links_txt(request: Request, username: str) -> PlainTextResponse:
        user = await _require_user(request)
        db = await _get_db(request)
        panel = await _get_active_panel(request, user)
        if panel is None:
            raise HTTPException(status_code=400, detail="No panel selected")

        client = await _get_panel_client(request, panel)
        api_user = await client.get_user(username)
        schedule = await db.get_schedule(username=username, panel_id=panel.id)

        resolved_links = await _resolve_links(request, panel, api_user)
        now_dt = datetime.now(tz=UTC)
        next_reset_dt = None
        if schedule and schedule.enabled:
            next_reset_dt = datetime.fromtimestamp(int(schedule.next_run_at), tz=UTC)

        header_lines = [
            f"panel={panel.name}",
            f"username={api_user.get('username')}",
            f"generated_at={format_dt(now_dt, settings.timezone)}",
        ]
        if next_reset_dt is not None:
            header_lines.append(f"next_reset_at={format_dt(next_reset_dt, settings.timezone)}")
        sub_url = str(api_user.get('subscription_url', '')).strip()
        if sub_url:
            header_lines.append(f"subscription_url={sub_url}")

        content = "\n".join(header_lines) + "\n\n" + "\n".join(resolved_links) + "\n"
        filename = f"configs_{username}.txt"
        return PlainTextResponse(content, headers={"Content-Disposition": f'attachment; filename="{filename}"'})

    @app.post("/users/{username}/revoke")
    async def user_revoke(request: Request, username: str) -> RedirectResponse:
        user = await _require_user(request)
        db = await _get_db(request)
        panel = await _get_active_panel(request, user)
        if panel is None:
            return _redirect("/panels?msg=Add+a+panel+first")

        client = await _get_panel_client(request, panel)
        await client.revoke_user_subscription(username)

        sched = await db.get_schedule(username=username, panel_id=panel.id)
        if sched and sched.enabled:
            next_run = int(time.time()) + int(sched.interval_minutes) * 60
            await db.mark_schedule_result(
                username=username,
                panel_id=panel.id,
                next_run_at=next_run,
                last_run_at=int(time.time()),
                last_error=None,
            )
        return _redirect(f"/users/{username}?msg=revoke_sub+done")

    @app.post("/users/{username}/reset-usage")
    async def user_reset_usage(request: Request, username: str) -> RedirectResponse:
        user = await _require_user(request)
        panel = await _get_active_panel(request, user)
        if panel is None:
            return _redirect("/panels?msg=Add+a+panel+first")

        client = await _get_panel_client(request, panel)
        await client.reset_user_data_usage(username)
        return _redirect(f"/users/{username}?msg=usage+reset+done")

    @app.post("/users/{username}/schedule")
    async def user_schedule(request: Request, username: str, interval: str = Form(...)) -> RedirectResponse:
        user = await _require_user(request)
        db = await _get_db(request)
        panel = await _get_active_panel(request, user)
        if panel is None:
            return _redirect("/panels?msg=Add+a+panel+first")

        interval_minutes = _parse_interval_minutes(interval)
        if interval_minutes is None:
            return _redirect_msg(f"/users/{username}", "invalid interval (use 7 or 2.30)")
        next_run_at = int(time.time()) + int(interval_minutes) * 60
        await db.set_schedule(
            username=username,
            panel_id=panel.id,
            interval_minutes=int(interval_minutes),
            next_run_at=next_run_at,
            enabled=True,
        )
        binding = await db.get_binding(username=username, panel_id=panel.id)
        chat_id = binding.chat_id if binding else panel.default_chat_id
        if chat_id is not None:
            next_dt = datetime.fromtimestamp(int(next_run_at), tz=UTC)
            text = "\n".join(
                [
                    "✅ زمان‌بندی ریست فعال شد",
                    f"👤 یوزر: {username}",
                    f"⏱ هر {_format_interval_minutes(int(interval_minutes))} (ساعت.دقیقه)",
                    f"⏳ ریست بعدی: {format_tehran_hour(next_dt, 'Asia/Tehran')}",
                    f"🗓 شمسی: {format_jalali_datetime(next_dt, settings.timezone)}",
                ]
            )
            await _try_send_telegram_message(request.app, chat_id=int(chat_id), text=text)

        return _redirect_msg(f"/users/{username}?open_cfg=1", f"scheduled {_format_interval_minutes(int(interval_minutes))}")

    @app.post("/users/{username}/unschedule")
    async def user_unschedule(request: Request, username: str, open_cfg: int = 0) -> RedirectResponse:
        user = await _require_user(request)
        db = await _get_db(request)
        panel = await _get_active_panel(request, user)
        if panel is None:
            return _redirect("/panels?msg=Add+a+panel+first")
        await db.disable_schedule(username=username, panel_id=panel.id)
        target = f"/users/{username}"
        if int(open_cfg or 0) == 1:
            target = f"/users/{username}?open_cfg=1"
        return _redirect_msg(target, "unscheduled")

    @app.post("/users/{username}/schedule-config")
    async def user_schedule_config(
        request: Request,
        username: str,
        message_template: str = Form(""),
        link_keys: list[str] = Form([]),
        button_templates: list[str] = Form([]),
    ) -> RedirectResponse:
        user = await _require_user(request)
        db = await _get_db(request)
        panel = await _get_active_panel(request, user)
        if panel is None:
            return _redirect("/panels?msg=Add+a+panel+first")

        template = (message_template or "").strip() or None
        keys = [str(k).strip() for k in (link_keys or []) if str(k).strip()]
        unique_keys: list[str] = []
        seen: set[str] = set()
        for k in keys:
            if k in seen:
                continue
            seen.add(k)
            unique_keys.append(k)

        raw_buttons = [str(x or "") for x in (button_templates or [])]
        button_tpls = [x.strip() for x in raw_buttons[:3]] if raw_buttons else None

        await db.set_schedule_config(
            username=username,
            panel_id=panel.id,
            message_template=template,
            selected_link_keys=unique_keys or None,
            button_templates=button_tpls,
        )
        return _redirect_msg(f"/users/{username}", "Schedule config saved")

    @app.post("/users/{username}/schedule-config/test")
    async def user_schedule_config_test(
        request: Request,
        username: str,
        message_template: str = Form(""),
        link_keys: list[str] = Form([]),
        button_templates: list[str] = Form([]),
    ) -> RedirectResponse:
        user = await _require_user(request)
        db = await _get_db(request)
        panel = await _get_active_panel(request, user)
        if panel is None:
            return _redirect("/panels?msg=Add+a+panel+first")

        telegram_cfg = await db.get_telegram_config()
        if not telegram_cfg.bot_token:
            return _redirect_msg(f"/users/{username}?open_cfg=1", "Set Telegram bot token first")

        admins = sorted(_parse_int_set(telegram_cfg.admin_user_ids or ""))
        if not admins:
            return _redirect_msg(f"/users/{username}?open_cfg=1", "Set Telegram admin user IDs first")

        client = await _get_panel_client(request, panel)
        api_user = await client.get_user(username)
        usage = await client.get_user_usage(username)
        schedule = await db.get_schedule(username=username, panel_id=panel.id)

        now_dt = datetime.now(tz=UTC)
        next_dt = None
        if schedule and schedule.enabled:
            next_dt = datetime.fromtimestamp(int(schedule.next_run_at), tz=UTC)

        links_all = await _resolve_links(request, panel, api_user)
        _groups, link_items = _build_link_items(links_all)

        keys = [str(k).strip() for k in (link_keys or []) if str(k).strip()]
        selected_set = set(keys)
        links_selected = links_all
        if selected_set:
            filtered = [
                it["url"]
                for it in link_items
                if it.get("key") in selected_set
                or it.get("compat_key") in selected_set
                or it.get("legacy_key") in selected_set
            ]
            if filtered:
                links_selected = filtered

        inbound_names = _extract_inbound_names(api_user)
        inbound_name = ", ".join(inbound_names) if inbound_names else "-"

        used = int(api_user.get("used_traffic") or 0)
        data_limit_raw = api_user.get("data_limit")
        data_limit = None
        try:
            if data_limit_raw not in (None, "", 0, "0"):
                data_limit = int(data_limit_raw)
        except Exception:
            data_limit = None
        if data_limit is not None and data_limit <= 0:
            data_limit = None
        remaining = None if data_limit is None else max(0, int(data_limit) - int(used))

        now_local = now_dt.astimezone(_get_tz(settings.timezone))
        date_gregorian = now_local.strftime("%Y-%m-%d")

        ctx = {
            "panel_name": panel.name,
            "username": str(api_user.get("username") or username),
            "inbound_name": inbound_name,
            "date_jalali": format_jalali_date(now_dt, settings.timezone),
            "date_gregorian": date_gregorian,
            "traffic_used_human": format_bytes(used),
            "traffic_limit_human": format_bytes(data_limit),
            "traffic_remaining_human": format_bytes(remaining),
            "next_reset_at": format_tehran_hour(next_dt, "Asia/Tehran"),
            "next_reset_at_jalali": format_jalali_datetime(next_dt, settings.timezone) if next_dt is not None else "-",
            "configs": _format_links_markdown(links_selected),
            "configs_count": len(links_selected),
            "links": _format_links_markdown(links_selected),
            "links_count": len(links_selected),
        }

        template = (message_template or "").strip() or DEFAULT_SCHEDULE_MESSAGE_TEMPLATE
        message = _render_message_template(template, ctx).strip()
        if not message:
            message = build_report_message(
                user=api_user,
                usage=usage,
                tz_name=settings.timezone,
                now=now_dt,
                next_reset_at=next_dt,
                interval_hours=None,
                reason=f"TEST (panel: {panel.name})",
            )
        message = "🧪 TEST پیام (preview)\n\n" + message

        raw_buttons = [str(x or "") for x in (button_templates or [])]
        button_tpls = [x.strip() for x in raw_buttons[:3]] if raw_buttons else None
        reply_markup = _build_telegram_info_buttons(button_tpls, ctx)

        bot = Bot(token=telegram_cfg.bot_token)
        sent = 0
        failed = 0
        for chat_id in admins:
            try:
                await _send_telegram_text(bot, chat_id=int(chat_id), text=message, reply_markup=reply_markup)
                sent += 1
            except Exception:
                failed += 1
                logger.exception("telegram test send failed (chat_id=%s)", chat_id)

        return _redirect_msg(
            f"/users/{username}?open_cfg=1",
            f"Test sent to {sent} admin(s). Failed: {failed}",
        )

    @app.post("/users/{username}/bind")
    async def user_bind(request: Request, username: str, chat_id: int = Form(...)) -> RedirectResponse:
        user = await _require_user(request)
        db = await _get_db(request)
        panel = await _get_active_panel(request, user)
        if panel is None:
            return _redirect("/panels?msg=Add+a+panel+first")
        await db.upsert_binding(username=username, panel_id=panel.id, chat_id=int(chat_id), user_id=None)
        return _redirect(f"/users/{username}?msg=binding+saved")

    @app.post("/users/{username}/unbind")
    async def user_unbind(request: Request, username: str) -> RedirectResponse:
        user = await _require_user(request)
        db = await _get_db(request)
        panel = await _get_active_panel(request, user)
        if panel is None:
            return _redirect("/panels?msg=Add+a+panel+first")
        await db.delete_binding(username=username, panel_id=panel.id)
        return _redirect(f"/users/{username}?msg=unbound")

    @app.get("/schedules", response_class=HTMLResponse)
    async def schedules_page(request: Request, msg: str = "") -> HTMLResponse:
        user = await _require_user(request)
        db = await _get_db(request)
        panels = await _user_panels(db, user)
        items = await db.list_schedules_for_owner(owner_user_id=user.id, limit=200)
        panel_map = {p.id: p for p in panels}

        def epoch_to_dt(value: int | None) -> datetime | None:
            if value is None:
                return None
            return datetime.fromtimestamp(int(value), tz=UTC)

        return templates.TemplateResponse(
            request=request,
            name="schedules.html",
            context={
                "user": user,
                "panels": panels,
                "items": items,
                "panel_map": panel_map,
                "epoch_to_dt": epoch_to_dt,
                "format_dt": format_dt,
                "format_interval": _format_interval_minutes,
                "tz_name": settings.timezone,
                "msg": msg,
            },
        )

    @app.get("/telegram", response_class=HTMLResponse)
    async def telegram_page(request: Request, msg: str = "") -> HTMLResponse:
        user = await _require_user(request)
        db = await _get_db(request)
        cfg = await db.get_telegram_config()
        polling_enabled = _parse_bool(await db.get_kv("telegram_polling_enabled"))
        polling_offset = (await db.get_kv("telegram_polling_offset") or "").strip()
        polling_last_error = (await db.get_kv("telegram_polling_last_error") or "").strip()
        info = None
        if cfg.bot_token:
            try:
                bot = Bot(token=cfg.bot_token)
                webhook_info = await bot.get_webhook_info()
                info = webhook_info.to_dict() if hasattr(webhook_info, "to_dict") else str(webhook_info)
            except Exception as e:
                info = {"error": f"{type(e).__name__}: {str(e)}"}
        return templates.TemplateResponse(
            request=request,
            name="telegram.html",
            context={
                "user": user,
                "cfg": cfg,
                "info": info,
                "polling_enabled": polling_enabled,
                "polling_offset": polling_offset,
                "polling_last_error": polling_last_error,
                "msg": msg,
                "webhook_endpoint": "/telegram/webhook",
            },
        )

    @app.post("/telegram/save")
    async def telegram_save(
        request: Request,
        bot_token: str = Form(""),
        admin_user_ids: str = Form(""),
        webhook_url: str = Form(""),
    ) -> RedirectResponse:
        await _require_user(request)
        db = await _get_db(request)
        await db.set_telegram_config(
            bot_token=bot_token.strip() or None,
            admin_user_ids=admin_user_ids.strip() or None,
            webhook_url=webhook_url.strip() or None,
        )
        return _redirect("/telegram?msg=Saved")

    @app.post("/telegram/set-webhook")
    async def telegram_set_webhook(request: Request) -> RedirectResponse:
        await _require_user(request)
        db = await _get_db(request)
        cfg = await db.get_telegram_config()
        if not cfg.bot_token:
            return _redirect("/telegram?msg=Set+bot+token+first")
        if not cfg.webhook_url:
            return _redirect("/telegram?msg=Set+webhook_url+first")
        bot = Bot(token=cfg.bot_token)
        await bot.set_webhook(url=cfg.webhook_url)
        await db.set_kv("telegram_polling_enabled", "0")
        return _redirect("/telegram?msg=Webhook+set")

    @app.post("/telegram/delete-webhook")
    async def telegram_delete_webhook(request: Request) -> RedirectResponse:
        await _require_user(request)
        db = await _get_db(request)
        cfg = await db.get_telegram_config()
        if not cfg.bot_token:
            return _redirect("/telegram?msg=Set+bot+token+first")
        bot = Bot(token=cfg.bot_token)
        await bot.delete_webhook()
        return _redirect("/telegram?msg=Webhook+deleted")

    @app.post("/telegram/polling/enable")
    async def telegram_polling_enable(request: Request) -> RedirectResponse:
        await _require_user(request)
        db = await _get_db(request)
        cfg = await db.get_telegram_config()
        if not cfg.bot_token:
            return _redirect_msg("/telegram", "Set bot token first")
        bot = Bot(token=cfg.bot_token)
        try:
            await bot.delete_webhook(drop_pending_updates=True)
        except Exception:
            pass
        await db.set_kv("telegram_polling_enabled", "1")
        await db.set_kv("telegram_polling_offset", "")
        await db.set_kv("telegram_polling_last_error", "")
        return _redirect_msg("/telegram", "Polling enabled (no webhook). Send /whoami to your bot.")

    @app.post("/telegram/polling/disable")
    async def telegram_polling_disable(request: Request) -> RedirectResponse:
        await _require_user(request)
        db = await _get_db(request)
        await db.set_kv("telegram_polling_enabled", "0")
        return _redirect_msg("/telegram", "Polling disabled")

    @app.post("/telegram/polling/poll-now")
    async def telegram_polling_poll_now(request: Request) -> RedirectResponse:
        await _require_user(request)
        processed = await _telegram_poll_tick(request.app)
        return _redirect_msg("/telegram", f"Polled updates: {processed}")

    @app.post("/telegram/webhook")
    async def telegram_webhook(request: Request) -> dict[str, Any]:
        db = await _get_db(request)
        cfg = await db.get_telegram_config()
        if not cfg.bot_token:
            return {"ok": False, "error": "bot_token not configured"}

        update = await request.json()
        bot = Bot(token=cfg.bot_token)
        await _telegram_handle_update(db=db, cfg=cfg, bot=bot, update=update)
        return {"ok": True}

    @app.exception_handler(MarzbanApiError)
    async def _marzban_error_handler(request: Request, exc: MarzbanApiError):  # type: ignore[override]
        return _redirect_msg("/users", f"API error: {_clean_msg(str(exc))}")

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {"ok": True}

    return app
