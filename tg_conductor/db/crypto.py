"""AES-256-GCM encrypt / decrypt helpers for at-rest secrets.

Wire format (binary, see design.md D7): ``nonce(12) || ciphertext || tag(16)``.
The DB column adds a base64 layer to keep TEXT columns ASCII-safe; the
crypto layer itself stays in bytes.

The key is read from :func:`tg_conductor.config.settings.get_settings`
unless the caller injects one (useful for tests and future key rotation).
"""

from __future__ import annotations

import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from tg_conductor.config.settings import get_settings

_NONCE_LEN = 12


def _resolve_key(key: bytes | None) -> bytes:
    if key is not None:
        if len(key) != 32:
            raise ValueError(f"AES-256 key must be 32 bytes, got {len(key)}")
        return key
    return get_settings().master_key_bytes


def encrypt(plaintext: bytes, *, key: bytes | None = None) -> bytes:
    """Encrypt with a fresh random nonce; return ``nonce || ciphertext || tag``."""
    nonce = os.urandom(_NONCE_LEN)
    body = AESGCM(_resolve_key(key)).encrypt(nonce, plaintext, None)
    return nonce + body


def decrypt(ciphertext: bytes, *, key: bytes | None = None) -> bytes:
    """Reverse of :func:`encrypt`. Raises ``InvalidTag`` on tamper / wrong key."""
    if len(ciphertext) < _NONCE_LEN + 16:
        raise ValueError("ciphertext too short to contain nonce + GCM tag")
    nonce, body = ciphertext[:_NONCE_LEN], ciphertext[_NONCE_LEN:]
    return AESGCM(_resolve_key(key)).decrypt(nonce, body, None)
