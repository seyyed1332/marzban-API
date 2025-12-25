from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from typing import Any

import httpx
from telegram import Bot

from .db import Schedule
from .reports import (
    build_links_document,
    build_report_message,
    resolve_links_from_api_user,
    resolve_links_from_subscription_payload,
)
from .runtime import Runtime


async def _fetch_subscription_payload(
    *,
    http: httpx.AsyncClient,
    subscription_url: str,
) -> str | None:
    if not subscription_url:
        return None
    try:
        resp = await http.get(subscription_url)
        resp.raise_for_status()
        return resp.text
    except Exception:
        return None


async def _send_report(
    *,
    bot: Bot,
    chat_id: int,
    message: str,
    document,
) -> None:
    await bot.send_message(chat_id=chat_id, text=message)
    if document is not None:
        await bot.send_document(chat_id=chat_id, document=document)


async def _resolve_links(runtime: Runtime, user: dict[str, Any]) -> list[str]:
    api_links = resolve_links_from_api_user(user)
    subscription_url = str(user.get("subscription_url", "")).strip()
    payload = await _fetch_subscription_payload(http=runtime.public_http, subscription_url=subscription_url)
    if payload:
        links = resolve_links_from_subscription_payload(payload)
        if links and not (len(links) == 1 and "://" not in links[0]):
            return links
    return api_links


async def _get_target_chat_id(runtime: Runtime, username: str) -> int | None:
    binding = await runtime.db.get_binding(username=username)
    if binding is not None:
        return binding.chat_id
    raw = await runtime.db.get_kv("default_chat_id")
    if raw is None:
        return None
    try:
        return int(raw)
    except Exception:
        return None


async def run_due_schedules(bot: Bot, runtime: Runtime) -> None:
    now_epoch = int(time.time())
    due = await runtime.db.get_due_schedules(now=now_epoch)
    for sched in due:
        await _run_single_schedule(bot=bot, runtime=runtime, sched=sched, now_epoch=now_epoch)


async def _run_single_schedule(*, bot: Bot, runtime: Runtime, sched: Schedule, now_epoch: int) -> None:
    username = sched.username
    lock = runtime.locks.setdefault(username, asyncio.Lock())
    if lock.locked():
        return

    async with lock:
        interval_hours = int(sched.interval_hours)
        interval_seconds = max(1, interval_hours) * 3600
        next_run_epoch = int(time.time()) + interval_seconds
        last_run_epoch = int(time.time())

        chat_id = await _get_target_chat_id(runtime, username)
        if chat_id is None:
            await runtime.db.mark_schedule_result(
                username=username,
                next_run_at=next_run_epoch,
                last_run_at=last_run_epoch,
                last_error="No chat_id (use /start once or /bind username)",
            )
            return

        try:
            user = await runtime.marzban.revoke_user_subscription(username)
            usage = await runtime.marzban.get_user_usage(username)
            now_dt = datetime.now(tz=UTC)
            next_reset_dt = datetime.fromtimestamp(next_run_epoch, tz=UTC)
            message = build_report_message(
                user=user,
                usage=usage,
                tz_name=runtime.settings.timezone,
                now=now_dt,
                next_reset_at=next_reset_dt,
                interval_hours=interval_hours,
                reason="scheduled revoke_sub",
            )

            links = await _resolve_links(runtime, user)
            doc = build_links_document(
                user=user,
                resolved_links=links,
                tz_name=runtime.settings.timezone,
                now=now_dt,
                next_reset_at=next_reset_dt,
            )
            await _send_report(bot=bot, chat_id=chat_id, message=message, document=doc)

            await runtime.db.mark_schedule_result(
                username=username,
                next_run_at=next_run_epoch,
                last_run_at=last_run_epoch,
                last_error=None,
            )
        except Exception as e:
            retry_epoch = int(time.time()) + 300
            await runtime.db.mark_schedule_result(
                username=username,
                next_run_at=retry_epoch,
                last_run_at=last_run_epoch,
                last_error=str(e)[:500],
            )


async def scheduler_loop(bot: Bot, runtime: Runtime) -> None:
    try:
        while True:
            await run_due_schedules(bot, runtime)
            await asyncio.sleep(max(5, int(runtime.settings.poll_interval_seconds)))
    except asyncio.CancelledError:
        raise
