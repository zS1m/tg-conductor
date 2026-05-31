"""§4.2 / §4.3 — AES-GCM round-trip, tamper detection, plaintext leakage check."""

from __future__ import annotations

import os

import pytest
from cryptography.exceptions import InvalidTag

from tg_conductor.db.crypto import decrypt, encrypt

_SESSION_LIKE = (
    b"BAEgAQB0eXBlPXVzZXIuc2Vzc2lvbi1kdW1teS1zZWNyZXQtaGV4LXBhYmxvLXdh"
    b"a2Vfb3BfdGhpc19sb29rc19saWtlX2FfdGdfc2Vzc2lvbl9zdHJpbmc="
)


@pytest.fixture
def k1() -> bytes:
    return b"\x01" * 32


@pytest.fixture
def k2() -> bytes:
    return b"\x02" * 32


def test_round_trip(k1: bytes) -> None:
    ct = encrypt(_SESSION_LIKE, key=k1)
    assert decrypt(ct, key=k1) == _SESSION_LIKE


def test_each_encrypt_uses_fresh_nonce(k1: bytes) -> None:
    ct_a = encrypt(_SESSION_LIKE, key=k1)
    ct_b = encrypt(_SESSION_LIKE, key=k1)
    assert ct_a != ct_b, "same plaintext + key must produce different ciphertext"
    assert ct_a[:12] != ct_b[:12], "nonces must differ"


def test_tamper_in_ciphertext_body_is_detected(k1: bytes) -> None:
    ct = bytearray(encrypt(_SESSION_LIKE, key=k1))
    ct[len(ct) // 2] ^= 0x01
    with pytest.raises(InvalidTag):
        decrypt(bytes(ct), key=k1)


def test_tamper_in_gcm_tag_is_detected(k1: bytes) -> None:
    ct = bytearray(encrypt(_SESSION_LIKE, key=k1))
    ct[-1] ^= 0xFF
    with pytest.raises(InvalidTag):
        decrypt(bytes(ct), key=k1)


def test_wrong_key_fails(k1: bytes, k2: bytes) -> None:
    ct = encrypt(_SESSION_LIKE, key=k1)
    with pytest.raises(InvalidTag):
        decrypt(ct, key=k2)


def test_truncated_ciphertext_rejected(k1: bytes) -> None:
    with pytest.raises(ValueError, match="too short"):
        decrypt(b"\x00" * 10, key=k1)


def test_invalid_key_length_rejected() -> None:
    with pytest.raises(ValueError, match="32 bytes"):
        encrypt(b"x", key=b"\x00" * 16)


def test_plaintext_does_not_appear_in_ciphertext(k1: bytes) -> None:
    """Substring scan: no 8-byte+ window of plaintext leaks through GCM."""
    plaintext = os.urandom(8) + _SESSION_LIKE + os.urandom(8)
    ct = encrypt(plaintext, key=k1)
    assert plaintext not in ct
    window = 8
    for i in range(len(plaintext) - window + 1):
        assert plaintext[i : i + window] not in ct, (
            f"plaintext leak at offset {i}: {plaintext[i : i + window]!r}"
        )


def test_uses_settings_master_key_by_default(master_key: str) -> None:
    """When no explicit key is supplied, falls back to APP_MASTER_KEY."""
    # conftest's autouse fixture already cleared the get_settings cache.
    ct = encrypt(b"hello")
    assert decrypt(ct) == b"hello"
