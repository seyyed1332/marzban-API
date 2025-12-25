from __future__ import annotations

import os
import secrets
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    app_secret_key: str

    web_host: str
    web_port: int
    signup_code: str | None

    timezone: str
    poll_interval_seconds: int
    db_path: str


def _parse_bool(value: str, *, default: bool) -> bool:
    raw = (value or "").strip().lower()
    if raw == "":
        return default
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {value!r}")


def load_settings() -> Settings:
    load_dotenv()

    app_secret_key = os.getenv("APP_SECRET_KEY", "").strip()
    if not app_secret_key:
        secret_path = Path(os.getenv("APP_SECRET_KEY_FILE", os.path.join("data", "app_secret.key")))
        try:
            if secret_path.exists():
                app_secret_key = secret_path.read_text(encoding="utf-8").strip()
            else:
                secret_path.parent.mkdir(parents=True, exist_ok=True)
                app_secret_key = secrets.token_urlsafe(48)
                secret_path.write_text(app_secret_key, encoding="utf-8")
        except Exception:
            app_secret_key = ""

    web_host = os.getenv("WEB_HOST", "127.0.0.1").strip() or "127.0.0.1"
    web_port = int(os.getenv("WEB_PORT", "8000"))
    signup_code = os.getenv("SIGNUP_CODE", "").strip() or None

    timezone = os.getenv("TIMEZONE", "Asia/Tehran").strip() or "UTC"
    poll_interval_seconds = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))
    db_path = os.getenv("DB_PATH", os.path.join("data", "bot.sqlite3")).strip()

    if not app_secret_key:
        raise RuntimeError("Missing required app secret (set APP_SECRET_KEY or provide APP_SECRET_KEY_FILE)")

    return Settings(
        app_secret_key=app_secret_key,
        web_host=web_host,
        web_port=web_port,
        signup_code=signup_code,
        timezone=timezone,
        poll_interval_seconds=poll_interval_seconds,
        db_path=db_path,
    )
