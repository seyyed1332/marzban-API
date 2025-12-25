from __future__ import annotations

import base64
import re


_URI_SCHEMES = (
    "vmess://",
    "vless://",
    "trojan://",
    "ss://",
    "hysteria://",
    "hy2://",
    "tuic://",
    "wireguard://",
)


_BASE64_RE = re.compile(r"^[A-Za-z0-9+/_-]+={0,2}$")


def _maybe_base64_decode(text: str) -> str | None:
    candidate = "".join(text.split())
    if len(candidate) < 16 or not _BASE64_RE.match(candidate):
        return None

    padded = candidate + ("=" * (-len(candidate) % 4))
    for decoder in (base64.b64decode, base64.urlsafe_b64decode):
        try:
            decoded = decoder(padded).decode("utf-8", errors="ignore")
        except Exception:
            continue
        if any(s in decoded for s in _URI_SCHEMES) or "://" in decoded:
            return decoded
    return None


def resolve_subscription_to_links(payload_text: str) -> list[str]:
    """
    Tries to "resolve" a subscription payload into individual config links.

    The Marzban subscription can be:
    - plain text with one URI per line
    - base64 encoded list of URIs
    - (less common) a YAML/JSON config for some clients
    """
    text = (payload_text or "").strip()
    if not text:
        return []

    if any(s in text for s in _URI_SCHEMES):
        return [line.strip() for line in text.splitlines() if line.strip()]

    decoded = _maybe_base64_decode(text)
    if decoded:
        return [line.strip() for line in decoded.splitlines() if line.strip()]

    # Not a classic subscription list; return the raw text as a single "config".
    return [text]

