from __future__ import annotations

import base64
import hashlib
import hmac
import os

from cryptography.fernet import Fernet, InvalidToken


def hash_password(password: str, *, iterations: int = 210_000) -> str:
    if not password:
        raise ValueError("Password is empty")
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return "pbkdf2_sha256$%d$%s$%s" % (
        iterations,
        base64.urlsafe_b64encode(salt).decode("ascii").rstrip("="),
        base64.urlsafe_b64encode(dk).decode("ascii").rstrip("="),
    )


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        algo, iterations_raw, salt_b64, dk_b64 = stored_hash.split("$", 3)
    except ValueError:
        return False
    if algo != "pbkdf2_sha256":
        return False
    try:
        iterations = int(iterations_raw)
        salt = base64.urlsafe_b64decode(salt_b64 + "==")
        expected = base64.urlsafe_b64decode(dk_b64 + "==")
    except Exception:
        return False

    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return hmac.compare_digest(dk, expected)


def _derive_fernet_key(secret: str) -> bytes:
    if not secret:
        raise ValueError("APP_SECRET_KEY is required for encryption")
    digest = hashlib.sha256(secret.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest)


def encrypt_text(secret: str, plaintext: str) -> str:
    f = Fernet(_derive_fernet_key(secret))
    token = f.encrypt(plaintext.encode("utf-8"))
    return token.decode("ascii")


def decrypt_text(secret: str, token: str) -> str:
    f = Fernet(_derive_fernet_key(secret))
    try:
        raw = f.decrypt(token.encode("ascii"))
    except InvalidToken as e:
        raise ValueError("Invalid encrypted value") from e
    return raw.decode("utf-8", errors="strict")

