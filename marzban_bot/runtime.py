from __future__ import annotations

import asyncio
from dataclasses import dataclass

import httpx

from .db import Database
from .marzban_client import MarzbanClient
from .settings import Settings


@dataclass
class Runtime:
    settings: Settings
    db: Database
    marzban: MarzbanClient
    public_http: httpx.AsyncClient
    locks: dict[str, asyncio.Lock]

