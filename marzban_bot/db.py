from __future__ import annotations

import os
import time
from dataclasses import dataclass
import json

import aiosqlite


@dataclass(frozen=True)
class AppUser:
    id: int
    username: str
    password_hash: str
    created_at: int


@dataclass(frozen=True)
class Panel:
    id: int
    owner_user_id: int
    name: str
    base_url: str
    admin_username: str
    admin_password_enc: str
    verify_ssl: bool
    default_chat_id: int | None
    created_at: int
    updated_at: int


@dataclass(frozen=True)
class TelegramConfig:
    bot_token: str | None
    admin_user_ids: str | None
    webhook_url: str | None
    updated_at: int


@dataclass(frozen=True)
class Binding:
    panel_id: int
    username: str
    chat_id: int
    user_id: int | None
    updated_at: int


@dataclass(frozen=True)
class Schedule:
    panel_id: int
    username: str
    interval_minutes: int
    next_run_at: int
    enabled: bool
    last_run_at: int | None
    last_error: str | None


@dataclass(frozen=True)
class ScheduleConfig:
    panel_id: int
    username: str
    message_template: str | None
    selected_link_keys: list[str]
    button_templates: list[str]
    updated_at: int


class Database:
    def __init__(self, path: str) -> None:
        self._path = path
        self._conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
        self._conn = await aiosqlite.connect(self._path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL;")
        await self._conn.execute("PRAGMA foreign_keys=ON;")
        await self._init_schema()

    async def aclose(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("Database is not connected")
        return self._conn

    async def _init_schema(self) -> None:
        await self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS kv (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL,
              updated_at INTEGER NOT NULL
            );
            """
        )

        await self._maybe_rename_legacy_table("bindings", expected_cols={"panel_id"})
        await self._maybe_rename_legacy_table("schedules", expected_cols={"panel_id"})

        await self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS app_users (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              username TEXT NOT NULL UNIQUE,
              password_hash TEXT NOT NULL,
              created_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS panels (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              owner_user_id INTEGER NOT NULL,
              name TEXT NOT NULL,
              base_url TEXT NOT NULL,
              admin_username TEXT NOT NULL,
              admin_password_enc TEXT NOT NULL,
              verify_ssl INTEGER NOT NULL DEFAULT 1,
              default_chat_id INTEGER,
              created_at INTEGER NOT NULL,
              updated_at INTEGER NOT NULL,
              FOREIGN KEY(owner_user_id) REFERENCES app_users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS telegram_config (
              id INTEGER PRIMARY KEY CHECK (id = 1),
              bot_token TEXT,
              admin_user_ids TEXT,
              webhook_url TEXT,
              updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS bindings (
              panel_id INTEGER NOT NULL,
              username TEXT NOT NULL,
              chat_id INTEGER NOT NULL,
              user_id INTEGER,
              updated_at INTEGER NOT NULL,
              PRIMARY KEY(panel_id, username),
              FOREIGN KEY(panel_id) REFERENCES panels(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS schedules (
              panel_id INTEGER NOT NULL,
              username TEXT NOT NULL,
              interval_hours INTEGER NOT NULL,
              interval_minutes INTEGER,
              next_run_at INTEGER NOT NULL,
              enabled INTEGER NOT NULL DEFAULT 1,
              last_run_at INTEGER,
              last_error TEXT,
              PRIMARY KEY(panel_id, username),
              FOREIGN KEY(panel_id) REFERENCES panels(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS schedule_configs (
              panel_id INTEGER NOT NULL,
              username TEXT NOT NULL,
              message_template TEXT,
              selected_link_keys TEXT,
              button_templates TEXT,
              updated_at INTEGER NOT NULL,
              PRIMARY KEY(panel_id, username),
              FOREIGN KEY(panel_id) REFERENCES panels(id) ON DELETE CASCADE
            );
            """
        )
        await self.conn.commit()

        await self._ensure_interval_minutes()
        await self._ensure_schedule_config_button_templates()

    async def _ensure_interval_minutes(self) -> None:
        if not await self._table_exists("schedules"):
            return
        cols = await self._table_columns("schedules")
        if "interval_minutes" in cols:
            await self.conn.execute(
                "UPDATE schedules SET interval_minutes = interval_hours * 60 WHERE interval_minutes IS NULL"
            )
            await self.conn.commit()
            return
        await self.conn.execute("ALTER TABLE schedules ADD COLUMN interval_minutes INTEGER;")
        await self.conn.execute("UPDATE schedules SET interval_minutes = interval_hours * 60 WHERE interval_minutes IS NULL;")
        await self.conn.commit()

    async def _ensure_schedule_config_button_templates(self) -> None:
        if not await self._table_exists("schedule_configs"):
            return
        cols = await self._table_columns("schedule_configs")
        if "button_templates" in cols:
            return
        await self.conn.execute("ALTER TABLE schedule_configs ADD COLUMN button_templates TEXT;")
        await self.conn.commit()

    async def _table_exists(self, name: str) -> bool:
        cur = await self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (name,),
        )
        row = await cur.fetchone()
        return row is not None

    async def _table_columns(self, name: str) -> set[str]:
        cur = await self.conn.execute(f"PRAGMA table_info({name})")
        rows = await cur.fetchall()
        cols: set[str] = set()
        for row in rows:
            cols.add(str(row["name"]))
        return cols

    async def _maybe_rename_legacy_table(self, name: str, *, expected_cols: set[str]) -> None:
        if not await self._table_exists(name):
            return
        cols = await self._table_columns(name)
        if expected_cols.issubset(cols):
            return
        legacy = f"{name}_legacy"
        if await self._table_exists(legacy):
            return
        await self.conn.execute(f"ALTER TABLE {name} RENAME TO {legacy}")
        await self.conn.commit()

    async def count_users(self) -> int:
        cur = await self.conn.execute("SELECT COUNT(*) AS c FROM app_users")
        row = await cur.fetchone()
        return int(row["c"]) if row else 0

    async def create_user(self, *, username: str, password_hash: str) -> AppUser:
        now = int(time.time())
        cur = await self.conn.execute(
            "INSERT INTO app_users(username, password_hash, created_at) VALUES(?, ?, ?)",
            (username, password_hash, now),
        )
        await self.conn.commit()
        user_id = int(cur.lastrowid)
        return AppUser(id=user_id, username=username, password_hash=password_hash, created_at=now)

    async def get_user_by_username(self, username: str) -> AppUser | None:
        cur = await self.conn.execute(
            "SELECT id, username, password_hash, created_at FROM app_users WHERE username=?",
            (username,),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        return AppUser(
            id=int(row["id"]),
            username=str(row["username"]),
            password_hash=str(row["password_hash"]),
            created_at=int(row["created_at"]),
        )

    async def get_user_by_id(self, user_id: int) -> AppUser | None:
        cur = await self.conn.execute(
            "SELECT id, username, password_hash, created_at FROM app_users WHERE id=?",
            (int(user_id),),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        return AppUser(
            id=int(row["id"]),
            username=str(row["username"]),
            password_hash=str(row["password_hash"]),
            created_at=int(row["created_at"]),
        )

    async def create_panel(
        self,
        *,
        owner_user_id: int,
        name: str,
        base_url: str,
        admin_username: str,
        admin_password_enc: str,
        verify_ssl: bool,
        default_chat_id: int | None = None,
    ) -> Panel:
        now = int(time.time())
        cur = await self.conn.execute(
            """
            INSERT INTO panels(
              owner_user_id, name, base_url, admin_username, admin_password_enc,
              verify_ssl, default_chat_id, created_at, updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(owner_user_id),
                name,
                base_url.rstrip("/"),
                admin_username,
                admin_password_enc,
                1 if verify_ssl else 0,
                default_chat_id,
                now,
                now,
            ),
        )
        await self.conn.commit()
        panel_id = int(cur.lastrowid)
        return Panel(
            id=panel_id,
            owner_user_id=int(owner_user_id),
            name=name,
            base_url=base_url.rstrip("/"),
            admin_username=admin_username,
            admin_password_enc=admin_password_enc,
            verify_ssl=bool(verify_ssl),
            default_chat_id=default_chat_id,
            created_at=now,
            updated_at=now,
        )

    async def list_panels(self, *, owner_user_id: int) -> list[Panel]:
        cur = await self.conn.execute(
            """
            SELECT id, owner_user_id, name, base_url, admin_username, admin_password_enc,
                   verify_ssl, default_chat_id, created_at, updated_at
            FROM panels
            WHERE owner_user_id=?
            ORDER BY id DESC
            """,
            (int(owner_user_id),),
        )
        rows = await cur.fetchall()
        out: list[Panel] = []
        for row in rows:
            out.append(
                Panel(
                    id=int(row["id"]),
                    owner_user_id=int(row["owner_user_id"]),
                    name=str(row["name"]),
                    base_url=str(row["base_url"]),
                    admin_username=str(row["admin_username"]),
                    admin_password_enc=str(row["admin_password_enc"]),
                    verify_ssl=bool(int(row["verify_ssl"])),
                    default_chat_id=None if row["default_chat_id"] is None else int(row["default_chat_id"]),
                    created_at=int(row["created_at"]),
                    updated_at=int(row["updated_at"]),
                )
            )
        return out

    async def get_panel(self, *, owner_user_id: int, panel_id: int) -> Panel | None:
        cur = await self.conn.execute(
            """
            SELECT id, owner_user_id, name, base_url, admin_username, admin_password_enc,
                   verify_ssl, default_chat_id, created_at, updated_at
            FROM panels
            WHERE owner_user_id=? AND id=?
            """,
            (int(owner_user_id), int(panel_id)),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        return Panel(
            id=int(row["id"]),
            owner_user_id=int(row["owner_user_id"]),
            name=str(row["name"]),
            base_url=str(row["base_url"]),
            admin_username=str(row["admin_username"]),
            admin_password_enc=str(row["admin_password_enc"]),
            verify_ssl=bool(int(row["verify_ssl"])),
            default_chat_id=None if row["default_chat_id"] is None else int(row["default_chat_id"]),
            created_at=int(row["created_at"]),
            updated_at=int(row["updated_at"]),
        )

    async def get_panel_by_id(self, panel_id: int) -> Panel | None:
        cur = await self.conn.execute(
            """
            SELECT id, owner_user_id, name, base_url, admin_username, admin_password_enc,
                   verify_ssl, default_chat_id, created_at, updated_at
            FROM panels
            WHERE id=?
            """,
            (int(panel_id),),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        return Panel(
            id=int(row["id"]),
            owner_user_id=int(row["owner_user_id"]),
            name=str(row["name"]),
            base_url=str(row["base_url"]),
            admin_username=str(row["admin_username"]),
            admin_password_enc=str(row["admin_password_enc"]),
            verify_ssl=bool(int(row["verify_ssl"])),
            default_chat_id=None if row["default_chat_id"] is None else int(row["default_chat_id"]),
            created_at=int(row["created_at"]),
            updated_at=int(row["updated_at"]),
        )

    async def update_panel(
        self,
        *,
        owner_user_id: int,
        panel_id: int,
        name: str,
        base_url: str,
        admin_username: str,
        admin_password_enc: str,
        verify_ssl: bool,
        default_chat_id: int | None,
    ) -> None:
        now = int(time.time())
        await self.conn.execute(
            """
            UPDATE panels
            SET name=?,
                base_url=?,
                admin_username=?,
                admin_password_enc=?,
                verify_ssl=?,
                default_chat_id=?,
                updated_at=?
            WHERE owner_user_id=? AND id=?
            """,
            (
                name,
                base_url.rstrip("/"),
                admin_username,
                admin_password_enc,
                1 if verify_ssl else 0,
                default_chat_id,
                now,
                int(owner_user_id),
                int(panel_id),
            ),
        )
        await self.conn.commit()

    async def delete_panel(self, *, owner_user_id: int, panel_id: int) -> None:
        await self.conn.execute(
            "DELETE FROM panels WHERE owner_user_id=? AND id=?",
            (int(owner_user_id), int(panel_id)),
        )
        await self.conn.commit()

    async def get_telegram_config(self) -> TelegramConfig:
        cur = await self.conn.execute(
            "SELECT bot_token, admin_user_ids, webhook_url, updated_at FROM telegram_config WHERE id=1"
        )
        row = await cur.fetchone()
        if row is None:
            return TelegramConfig(bot_token=None, admin_user_ids=None, webhook_url=None, updated_at=0)
        return TelegramConfig(
            bot_token=None if row["bot_token"] is None else str(row["bot_token"]),
            admin_user_ids=None if row["admin_user_ids"] is None else str(row["admin_user_ids"]),
            webhook_url=None if row["webhook_url"] is None else str(row["webhook_url"]),
            updated_at=int(row["updated_at"]),
        )

    async def set_telegram_config(
        self,
        *,
        bot_token: str | None,
        admin_user_ids: str | None,
        webhook_url: str | None,
    ) -> None:
        now = int(time.time())
        await self.conn.execute(
            """
            INSERT INTO telegram_config(id, bot_token, admin_user_ids, webhook_url, updated_at)
            VALUES(1, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              bot_token=excluded.bot_token,
              admin_user_ids=excluded.admin_user_ids,
              webhook_url=excluded.webhook_url,
              updated_at=excluded.updated_at
            """,
            (bot_token, admin_user_ids, webhook_url, now),
        )
        await self.conn.commit()

    async def set_kv(self, key: str, value: str) -> None:
        now = int(time.time())
        await self.conn.execute(
            """
            INSERT INTO kv(key, value, updated_at)
            VALUES(?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
              value=excluded.value,
              updated_at=excluded.updated_at
            """,
            (key, value, now),
        )
        await self.conn.commit()

    async def get_kv(self, key: str) -> str | None:
        cur = await self.conn.execute("SELECT value FROM kv WHERE key=?", (key,))
        row = await cur.fetchone()
        return None if row is None else str(row["value"])

    async def upsert_binding(
        self,
        *,
        username: str,
        chat_id: int,
        user_id: int | None,
        panel_id: int = 1,
    ) -> None:
        now = int(time.time())
        await self.conn.execute(
            """
            INSERT INTO bindings(panel_id, username, chat_id, user_id, updated_at)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(panel_id, username) DO UPDATE SET
              chat_id=excluded.chat_id,
              user_id=excluded.user_id,
              updated_at=excluded.updated_at
            """,
            (int(panel_id), username, chat_id, user_id, now),
        )
        await self.conn.commit()

    async def get_binding(self, *, username: str, panel_id: int = 1) -> Binding | None:
        cur = await self.conn.execute(
            "SELECT panel_id, username, chat_id, user_id, updated_at FROM bindings WHERE panel_id=? AND username=?",
            (int(panel_id), username),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        return Binding(
            panel_id=int(row["panel_id"]),
            username=str(row["username"]),
            chat_id=int(row["chat_id"]),
            user_id=None if row["user_id"] is None else int(row["user_id"]),
            updated_at=int(row["updated_at"]),
        )

    async def delete_binding(self, *, username: str, panel_id: int = 1) -> None:
        await self.conn.execute(
            "DELETE FROM bindings WHERE panel_id=? AND username=?",
            (int(panel_id), username),
        )
        await self.conn.commit()

    async def set_schedule(
        self,
        *,
        username: str,
        interval_minutes: int,
        next_run_at: int,
        enabled: bool = True,
        panel_id: int = 1,
    ) -> None:
        interval_minutes = int(interval_minutes)
        interval_hours = max(0, interval_minutes // 60)
        await self.conn.execute(
            """
            INSERT INTO schedules(panel_id, username, interval_hours, interval_minutes, next_run_at, enabled)
            VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(panel_id, username) DO UPDATE SET
              interval_hours=excluded.interval_hours,
              interval_minutes=excluded.interval_minutes,
              next_run_at=excluded.next_run_at,
              enabled=excluded.enabled
            """,
            (int(panel_id), username, interval_hours, interval_minutes, next_run_at, 1 if enabled else 0),
        )
        await self.conn.commit()

    async def disable_schedule(self, *, username: str, panel_id: int = 1) -> None:
        await self.conn.execute(
            "UPDATE schedules SET enabled=0 WHERE panel_id=? AND username=?",
            (int(panel_id), username),
        )
        await self.conn.commit()

    async def get_schedule(self, *, username: str, panel_id: int = 1) -> Schedule | None:
        cur = await self.conn.execute(
            """
            SELECT panel_id, username, COALESCE(interval_minutes, interval_hours * 60) AS interval_minutes, next_run_at, enabled, last_run_at, last_error
            FROM schedules
            WHERE panel_id=? AND username=?
            """,
            (int(panel_id), username),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        return Schedule(
            panel_id=int(row["panel_id"]),
            username=str(row["username"]),
            interval_minutes=int(row["interval_minutes"]),
            next_run_at=int(row["next_run_at"]),
            enabled=bool(int(row["enabled"])),
            last_run_at=None if row["last_run_at"] is None else int(row["last_run_at"]),
            last_error=None if row["last_error"] is None else str(row["last_error"]),
        )

    async def list_schedules(self, *, panel_id: int | None = None, limit: int = 50) -> list[Schedule]:
        if panel_id is None:
            cur = await self.conn.execute(
                """
                SELECT panel_id, username, COALESCE(interval_minutes, interval_hours * 60) AS interval_minutes, next_run_at, enabled, last_run_at, last_error
                FROM schedules
                ORDER BY next_run_at ASC
                LIMIT ?
                """,
                (int(limit),),
            )
        else:
            cur = await self.conn.execute(
                """
                SELECT panel_id, username, COALESCE(interval_minutes, interval_hours * 60) AS interval_minutes, next_run_at, enabled, last_run_at, last_error
                FROM schedules
                WHERE panel_id=?
                ORDER BY next_run_at ASC
                LIMIT ?
                """,
                (int(panel_id), int(limit)),
            )
        rows = await cur.fetchall()
        out: list[Schedule] = []
        for row in rows:
            out.append(
                Schedule(
                    panel_id=int(row["panel_id"]),
                    username=str(row["username"]),
                    interval_minutes=int(row["interval_minutes"]),
                    next_run_at=int(row["next_run_at"]),
                    enabled=bool(int(row["enabled"])),
                    last_run_at=None if row["last_run_at"] is None else int(row["last_run_at"]),
                    last_error=None if row["last_error"] is None else str(row["last_error"]),
                )
            )
        return out

    async def list_schedules_for_owner(
        self,
        *,
        owner_user_id: int,
        panel_id: int | None = None,
        limit: int = 50,
    ) -> list[Schedule]:
        if panel_id is None:
            sql = """
            SELECT s.panel_id, s.username, COALESCE(s.interval_minutes, s.interval_hours * 60) AS interval_minutes, s.next_run_at, s.enabled, s.last_run_at, s.last_error
            FROM schedules s
            JOIN panels p ON p.id = s.panel_id
            WHERE p.owner_user_id=?
            ORDER BY s.next_run_at ASC
            LIMIT ?
            """
            params = (int(owner_user_id), int(limit))
        else:
            sql = """
            SELECT s.panel_id, s.username, COALESCE(s.interval_minutes, s.interval_hours * 60) AS interval_minutes, s.next_run_at, s.enabled, s.last_run_at, s.last_error
            FROM schedules s
            JOIN panels p ON p.id = s.panel_id
            WHERE p.owner_user_id=? AND s.panel_id=?
            ORDER BY s.next_run_at ASC
            LIMIT ?
            """
            params = (int(owner_user_id), int(panel_id), int(limit))
        cur = await self.conn.execute(sql, params)
        rows = await cur.fetchall()
        out: list[Schedule] = []
        for row in rows:
            out.append(
                Schedule(
                    panel_id=int(row["panel_id"]),
                    username=str(row["username"]),
                    interval_minutes=int(row["interval_minutes"]),
                    next_run_at=int(row["next_run_at"]),
                    enabled=bool(int(row["enabled"])),
                    last_run_at=None if row["last_run_at"] is None else int(row["last_run_at"]),
                    last_error=None if row["last_error"] is None else str(row["last_error"]),
                )
            )
        return out

    async def get_due_schedules(self, *, now: int) -> list[Schedule]:
        cur = await self.conn.execute(
            """
            SELECT panel_id, username, COALESCE(interval_minutes, interval_hours * 60) AS interval_minutes, next_run_at, enabled, last_run_at, last_error
            FROM schedules
            WHERE enabled=1 AND next_run_at <= ?
            ORDER BY next_run_at ASC
            """,
            (now,),
        )
        rows = await cur.fetchall()
        out: list[Schedule] = []
        for row in rows:
            out.append(
                Schedule(
                    panel_id=int(row["panel_id"]),
                    username=str(row["username"]),
                    interval_minutes=int(row["interval_minutes"]),
                    next_run_at=int(row["next_run_at"]),
                    enabled=True,
                    last_run_at=None if row["last_run_at"] is None else int(row["last_run_at"]),
                    last_error=None if row["last_error"] is None else str(row["last_error"]),
                )
            )
        return out

    async def mark_schedule_result(
        self,
        *,
        username: str,
        next_run_at: int,
        last_run_at: int,
        last_error: str | None,
        panel_id: int = 1,
    ) -> None:
        await self.conn.execute(
            """
            UPDATE schedules
            SET next_run_at=?, last_run_at=?, last_error=?
            WHERE panel_id=? AND username=?
            """,
            (next_run_at, last_run_at, last_error, int(panel_id), username),
        )
        await self.conn.commit()

    async def get_schedule_config(self, *, username: str, panel_id: int = 1) -> ScheduleConfig | None:
        cur = await self.conn.execute(
            """
            SELECT panel_id, username, message_template, selected_link_keys, button_templates, updated_at
            FROM schedule_configs
            WHERE panel_id=? AND username=?
            """,
            (int(panel_id), username),
        )
        row = await cur.fetchone()
        if row is None:
            return None

        selected_raw = None if row["selected_link_keys"] is None else str(row["selected_link_keys"])
        selected: list[str] = []
        if selected_raw:
            try:
                parsed = json.loads(selected_raw)
                if isinstance(parsed, list):
                    selected = [str(x) for x in parsed if str(x).strip()]
            except Exception:
                selected = []

        buttons_raw = None if row["button_templates"] is None else str(row["button_templates"])
        buttons: list[str] = []
        if buttons_raw:
            try:
                parsed = json.loads(buttons_raw)
                if isinstance(parsed, list):
                    buttons = [str(x or "").strip() for x in parsed]
            except Exception:
                buttons = []

        return ScheduleConfig(
            panel_id=int(row["panel_id"]),
            username=str(row["username"]),
            message_template=None if row["message_template"] is None else str(row["message_template"]),
            selected_link_keys=selected,
            button_templates=buttons,
            updated_at=int(row["updated_at"]),
        )

    async def set_schedule_config(
        self,
        *,
        username: str,
        panel_id: int,
        message_template: str | None,
        selected_link_keys: list[str] | None,
        button_templates: list[str] | None,
    ) -> None:
        now = int(time.time())
        selected_json = None
        if selected_link_keys:
            selected_json = json.dumps(list(selected_link_keys), ensure_ascii=False)

        buttons_json = None
        if button_templates:
            buttons_json = json.dumps(list(button_templates), ensure_ascii=False)

        await self.conn.execute(
            """
            INSERT INTO schedule_configs(panel_id, username, message_template, selected_link_keys, button_templates, updated_at)
            VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(panel_id, username) DO UPDATE SET
              message_template=excluded.message_template,
              selected_link_keys=excluded.selected_link_keys,
              button_templates=excluded.button_templates,
              updated_at=excluded.updated_at
            """,
            (int(panel_id), username, message_template, selected_json, buttons_json, now),
        )
        await self.conn.commit()

    async def migrate_legacy_data(self, *, default_panel_id: int) -> None:
        if await self._table_exists("bindings_legacy"):
            await self.conn.execute(
                """
                INSERT OR IGNORE INTO bindings(panel_id, username, chat_id, user_id, updated_at)
                SELECT ?, username, chat_id, user_id, updated_at
                FROM bindings_legacy
                """,
                (int(default_panel_id),),
            )
            await self.conn.commit()

        if await self._table_exists("schedules_legacy"):
            await self.conn.execute(
                """
                INSERT OR IGNORE INTO schedules(panel_id, username, interval_hours, interval_minutes, next_run_at, enabled, last_run_at, last_error)
                SELECT ?, username, interval_hours, interval_hours * 60, next_run_at, enabled, last_run_at, last_error
                FROM schedules_legacy
                """,
                (int(default_panel_id),),
            )
            await self.conn.commit()
