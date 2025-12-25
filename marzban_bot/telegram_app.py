from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime
from typing import Any

import httpx
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from .db import Database
from .formatting import format_bytes, format_dt, parse_epoch_seconds
from .marzban_client import MarzbanApiError, MarzbanClient
from .reports import build_links_document, build_report_message, resolve_links_from_api_user, resolve_links_from_subscription_payload
from .runtime import Runtime
from .scheduler import scheduler_loop
from .settings import Settings

logger = logging.getLogger(__name__)


def _is_admin(settings: Settings, update: Update) -> bool:
    user = update.effective_user
    return bool(user and user.id in settings.telegram_admin_user_ids)


async def _require_admin(settings: Settings, update: Update) -> bool:
    if _is_admin(settings, update):
        return True
    if update.effective_message:
        await update.effective_message.reply_text("Not authorized.")
    return False


def _runtime(context: ContextTypes.DEFAULT_TYPE) -> Runtime:
    rt = context.application.bot_data.get("runtime")
    if not isinstance(rt, Runtime):
        raise RuntimeError("Runtime not initialized")
    return rt


async def cmd_whoami(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    chat = update.effective_chat
    if not (user and chat and update.effective_message):
        return
    await update.effective_message.reply_text(f"user_id={user.id}\nchat_id={chat.id}")


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not (update.effective_chat and update.effective_message):
        return

    settings: Settings = context.application.bot_data["settings"]
    if not await _require_admin(settings, update):
        return

    rt = _runtime(context)
    await rt.db.set_kv("default_chat_id", str(update.effective_chat.id))
    await update.effective_message.reply_text(
        "Admin chat saved as default.\nUse /help for commands."
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_message:
        return
    text = (
        "Commands:\n"
        "/whoami - show user_id/chat_id\n"
        "/users [search] - list users\n"
        "/user <username> - user info + schedule\n"
        "/links <username> - send links + usage report\n"
        "/revoke <username> - revoke_sub (reset sub token) + send new links\n"
        "/schedule <username> <hours> - auto revoke every N hours\n"
        "/schedule_all <hours> [search] - schedule all users\n"
        "/unschedule <username> - disable schedule\n"
        "/bind <username> [chat_id] - where to send scheduled reports\n"
        "/unbind <username> - remove binding\n"
        "/inbounds - list panel inbounds\n"
        "/status [username] - schedule status\n"
    )
    await update.effective_message.reply_text(text)


async def cmd_inbounds(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    if not await _require_admin(settings, update):
        return
    if not update.effective_message:
        return
    rt = _runtime(context)
    data = await rt.marzban.get_inbounds()
    lines = []
    for proto, tags in sorted(data.items()):
        lines.append(f"{proto}: {', '.join(tags) if tags else '-'}")
    await update.effective_message.reply_text("\n".join(lines) or "-")


async def cmd_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    if not await _require_admin(settings, update):
        return
    if not update.effective_message:
        return
    rt = _runtime(context)
    search = " ".join(context.args).strip() or None
    data = await rt.marzban.get_users(limit=30, search=search)
    users = data.get("users") if isinstance(data, dict) else None
    if not isinstance(users, list) or not users:
        await update.effective_message.reply_text("No users found.")
        return
    names = [str(u.get("username", "-")) for u in users]
    await update.effective_message.reply_text("\n".join(names[:30]))


async def cmd_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    if not await _require_admin(settings, update):
        return
    if not update.effective_message:
        return
    if not context.args:
        await update.effective_message.reply_text("Usage: /user <username>")
        return

    rt = _runtime(context)
    username = context.args[0].strip()
    user = await rt.marzban.get_user(username)
    sched = await rt.db.get_schedule(username=username)

    used = int(user.get("used_traffic") or 0)
    expire_dt = parse_epoch_seconds(user.get("expire"))
    raw_limit = user.get("data_limit")
    data_limit_display = None if raw_limit in (None, 0, "0") else int(raw_limit or 0)
    lines = [
        f"Username: {user.get('username')}",
        f"Status: {user.get('status')}",
        f"Traffic: {format_bytes(used)} / {format_bytes(data_limit_display)}",
        f"Expire: {format_dt(expire_dt, rt.settings.timezone)}",
        f"Sub URL: {user.get('subscription_url')}",
        f"Links: {len(user.get('links') or [])}",
    ]

    inbounds = user.get("inbounds")
    if isinstance(inbounds, dict) and inbounds:
        lines.append("Inbounds:")
        for proto, tags in sorted(inbounds.items()):
            if isinstance(tags, list):
                lines.append(f"- {proto}: {', '.join(map(str, tags)) if tags else '-'}")
    if sched is None:
        lines.append("Schedule: -")
    else:
        next_dt = datetime.fromtimestamp(int(sched.next_run_at), tz=UTC)
        lines.append(
            f"Schedule: {'enabled' if sched.enabled else 'disabled'}; every {sched.interval_hours}h; next {format_dt(next_dt, rt.settings.timezone)}"
        )
        if sched.last_error:
            lines.append(f"Last error: {sched.last_error}")

    await update.effective_message.reply_text("\n".join(lines))


async def _fetch_subscription_payload(rt: Runtime, url: str) -> str | None:
    url = (url or "").strip()
    if not url:
        return None
    try:
        resp = await rt.public_http.get(url)
        resp.raise_for_status()
        return resp.text
    except Exception:
        return None


async def _resolve_links(rt: Runtime, user: dict[str, Any]) -> list[str]:
    api_links = resolve_links_from_api_user(user)
    payload = await _fetch_subscription_payload(rt, str(user.get("subscription_url", "")))
    if payload:
        links = resolve_links_from_subscription_payload(payload)
        if links and not (len(links) == 1 and "://" not in links[0]):
            return links
    return api_links


async def _send_user_report_and_links(
    *,
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    user: dict[str, Any],
    reason: str,
) -> None:
    if not update.effective_chat:
        return

    rt = _runtime(context)
    username = str(user.get("username", "")).strip()
    sched = await rt.db.get_schedule(username=username) if username else None

    interval_hours = None
    next_reset_dt = None
    if sched and sched.enabled:
        interval_hours = int(sched.interval_hours)
        next_reset_dt = datetime.fromtimestamp(int(sched.next_run_at), tz=UTC)

    usage = None
    try:
        usage = await rt.marzban.get_user_usage(username)
    except Exception:
        usage = None

    now_dt = datetime.now(tz=UTC)
    message = build_report_message(
        user=user,
        usage=usage,
        tz_name=rt.settings.timezone,
        now=now_dt,
        next_reset_at=next_reset_dt,
        interval_hours=interval_hours,
        reason=reason,
    )

    links = await _resolve_links(rt, user)
    doc = build_links_document(
        user=user,
        resolved_links=links,
        tz_name=rt.settings.timezone,
        now=now_dt,
        next_reset_at=next_reset_dt,
    )

    await context.bot.send_message(chat_id=update.effective_chat.id, text=message)
    await context.bot.send_document(chat_id=update.effective_chat.id, document=doc)


async def cmd_links(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    if not await _require_admin(settings, update):
        return
    if not update.effective_message:
        return
    if not context.args:
        await update.effective_message.reply_text("Usage: /links <username>")
        return
    rt = _runtime(context)
    username = context.args[0].strip()
    user = await rt.marzban.get_user(username)
    await _send_user_report_and_links(update=update, context=context, user=user, reason="links requested")


async def cmd_revoke(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    if not await _require_admin(settings, update):
        return
    if not update.effective_message:
        return
    if not context.args:
        await update.effective_message.reply_text("Usage: /revoke <username>")
        return

    rt = _runtime(context)
    username = context.args[0].strip()
    user = await rt.marzban.revoke_user_subscription(username)

    # If a schedule exists, keep the cadence from "now".
    sched = await rt.db.get_schedule(username=username)
    if sched and sched.enabled:
        next_run = int(time.time()) + int(sched.interval_hours) * 3600
        await rt.db.mark_schedule_result(
            username=username,
            next_run_at=next_run,
            last_run_at=int(time.time()),
            last_error=None,
        )

    await _send_user_report_and_links(update=update, context=context, user=user, reason="manual revoke_sub")


async def cmd_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    if not await _require_admin(settings, update):
        return
    if not update.effective_message:
        return
    if len(context.args) < 2:
        await update.effective_message.reply_text("Usage: /schedule <username> <hours>")
        return

    rt = _runtime(context)
    username = context.args[0].strip()
    try:
        hours = int(context.args[1])
    except ValueError:
        await update.effective_message.reply_text("hours must be an integer")
        return
    if hours <= 0:
        await update.effective_message.reply_text("hours must be > 0")
        return

    next_run_at = int(time.time()) + hours * 3600
    await rt.db.set_schedule(username=username, interval_hours=hours, next_run_at=next_run_at, enabled=True)

    next_dt = datetime.fromtimestamp(next_run_at, tz=UTC)
    await update.effective_message.reply_text(
        f"Scheduled {username} every {hours}h. Next: {format_dt(next_dt, rt.settings.timezone)}"
    )


async def cmd_schedule_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    if not await _require_admin(settings, update):
        return
    if not update.effective_message:
        return
    if not context.args:
        await update.effective_message.reply_text("Usage: /schedule_all <hours> [search]")
        return

    rt = _runtime(context)
    try:
        hours = int(context.args[0])
    except ValueError:
        await update.effective_message.reply_text("hours must be an integer")
        return
    if hours <= 0:
        await update.effective_message.reply_text("hours must be > 0")
        return

    search = " ".join(context.args[1:]).strip() or None
    offset = 0
    limit = 100
    next_run_at = int(time.time()) + hours * 3600
    total_scheduled = 0

    while True:
        data = await rt.marzban.get_users(offset=offset, limit=limit, search=search)
        users = data.get("users") if isinstance(data, dict) else None
        if not isinstance(users, list) or not users:
            break

        for u in users:
            username = str(u.get("username", "")).strip()
            if not username:
                continue
            await rt.db.set_schedule(username=username, interval_hours=hours, next_run_at=next_run_at, enabled=True)
            total_scheduled += 1

        offset += limit
        if len(users) < limit:
            break

    next_dt = datetime.fromtimestamp(next_run_at, tz=UTC)
    await update.effective_message.reply_text(
        f"Scheduled {total_scheduled} users every {hours}h. Next: {format_dt(next_dt, rt.settings.timezone)}"
    )


async def cmd_unschedule(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    if not await _require_admin(settings, update):
        return
    if not update.effective_message:
        return
    if not context.args:
        await update.effective_message.reply_text("Usage: /unschedule <username>")
        return
    rt = _runtime(context)
    username = context.args[0].strip()
    await rt.db.disable_schedule(username=username)
    await update.effective_message.reply_text(f"Schedule disabled for {username}")


async def cmd_bind(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    if not await _require_admin(settings, update):
        return
    if not update.effective_chat or not update.effective_message:
        return
    if not context.args:
        await update.effective_message.reply_text("Usage: /bind <username> [chat_id]")
        return

    rt = _runtime(context)
    username = context.args[0].strip()
    if len(context.args) >= 2:
        try:
            chat_id = int(context.args[1])
        except ValueError:
            await update.effective_message.reply_text("chat_id must be an integer")
            return
    else:
        chat_id = int(update.effective_chat.id)
    user_id = update.effective_user.id if update.effective_user else None
    await rt.db.upsert_binding(username=username, chat_id=chat_id, user_id=user_id)
    await update.effective_message.reply_text(f"Bound {username} -> chat_id={chat_id}")


async def cmd_unbind(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    if not await _require_admin(settings, update):
        return
    if not update.effective_message:
        return
    if not context.args:
        await update.effective_message.reply_text("Usage: /unbind <username>")
        return
    rt = _runtime(context)
    username = context.args[0].strip()
    await rt.db.delete_binding(username=username)
    await update.effective_message.reply_text(f"Unbound {username}")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings: Settings = context.application.bot_data["settings"]
    if not await _require_admin(settings, update):
        return
    if not update.effective_message:
        return
    rt = _runtime(context)
    if context.args:
        username = context.args[0].strip()
        sched = await rt.db.get_schedule(username=username)
        if sched is None:
            await update.effective_message.reply_text("No schedule.")
            return
        next_dt = datetime.fromtimestamp(int(sched.next_run_at), tz=UTC)
        text = (
            f"{username}\n"
            f"enabled={sched.enabled}\n"
            f"interval_hours={sched.interval_hours}\n"
            f"next={format_dt(next_dt, rt.settings.timezone)}\n"
        )
        if sched.last_error:
            text += f"last_error={sched.last_error}\n"
        await update.effective_message.reply_text(text.strip())
        return

    schedules = await rt.db.list_schedules(limit=30)
    if not schedules:
        await update.effective_message.reply_text("No schedules.")
        return
    lines = []
    for s in schedules:
        next_dt = datetime.fromtimestamp(int(s.next_run_at), tz=UTC)
        lines.append(
            f"{s.username}: {'on' if s.enabled else 'off'}; {s.interval_hours}h; next {format_dt(next_dt, rt.settings.timezone)}"
        )
    await update.effective_message.reply_text("\n".join(lines))


async def _post_init(application: Application) -> None:
    settings: Settings = application.bot_data["settings"]
    if not settings.marzban_base_url or not settings.marzban_admin_username or not settings.marzban_admin_password:
        raise RuntimeError("MARZBAN_* env vars are required for polling bot")
    db = Database(settings.db_path)
    await db.connect()

    marzban = MarzbanClient(
        base_url=settings.marzban_base_url,
        username=settings.marzban_admin_username,
        password=settings.marzban_admin_password,
        verify_ssl=settings.marzban_verify_ssl,
    )
    await marzban.login()

    public_http = httpx.AsyncClient(timeout=httpx.Timeout(30.0), verify=settings.marzban_verify_ssl)
    runtime = Runtime(settings=settings, db=db, marzban=marzban, public_http=public_http, locks={})
    application.bot_data["runtime"] = runtime

    task = application.create_task(scheduler_loop(application.bot, runtime))
    application.bot_data["scheduler_task"] = task


async def _post_shutdown(application: Application) -> None:
    task = application.bot_data.get("scheduler_task")
    if isinstance(task, asyncio.Task):
        task.cancel()
        try:
            await task
        except Exception:
            pass

    runtime = application.bot_data.get("runtime")
    if isinstance(runtime, Runtime):
        await runtime.public_http.aclose()
        await runtime.marzban.aclose()
        await runtime.db.aclose()


async def _error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = context.error
    if isinstance(err, MarzbanApiError):
        logger.warning("MarzbanApiError: %s", err)
        if isinstance(update, Update) and update.effective_message:
            await update.effective_message.reply_text(f"API error: {err}")
        return

    logger.exception("Unhandled error", exc_info=err)
    if isinstance(update, Update) and update.effective_message:
        await update.effective_message.reply_text("Unhandled error. Check logs.")


def build_application(settings: Settings) -> Application:
    if not settings.telegram_bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required for polling bot")
    application = (
        Application.builder()
        .token(settings.telegram_bot_token)
        .post_init(_post_init)
        .post_shutdown(_post_shutdown)
        .build()
    )
    application.bot_data["settings"] = settings

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("whoami", cmd_whoami))
    application.add_handler(CommandHandler("users", cmd_users))
    application.add_handler(CommandHandler("user", cmd_user))
    application.add_handler(CommandHandler("links", cmd_links))
    application.add_handler(CommandHandler("revoke", cmd_revoke))
    application.add_handler(CommandHandler("schedule", cmd_schedule))
    application.add_handler(CommandHandler("schedule_all", cmd_schedule_all))
    application.add_handler(CommandHandler("unschedule", cmd_unschedule))
    application.add_handler(CommandHandler("bind", cmd_bind))
    application.add_handler(CommandHandler("unbind", cmd_unbind))
    application.add_handler(CommandHandler("inbounds", cmd_inbounds))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_error_handler(_error_handler)
    return application
