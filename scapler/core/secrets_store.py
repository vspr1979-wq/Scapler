"""Broker secret store (plan §8: secrets excluded from settings.json).

Windows: DPAPI (CryptProtectData/CryptUnprotectData, current-user scope) —
the packaged desktop path. Other platforms: 0600-perm JSON file with a loud
warning — development only, never the shipped configuration.

Files live under ``{data_dir}/secrets-{broker}``; the data dir is NEVER
inside the repo (default ~/.scapler) and is git-ignored.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

log = logging.getLogger(__name__)


def _dpapi_available() -> bool:
    return sys.platform == "win32"


def _protect(data: bytes) -> bytes:
    import ctypes
    import ctypes.wintypes as wt

    class DATA_BLOB(ctypes.Structure):
        _fields_ = [("cbData", wt.DWORD), ("pbData",
                                            ctypes.POINTER(ctypes.c_char))]

    blob_in = DATA_BLOB(len(data), ctypes.cast(ctypes.create_string_buffer(
        data, len(data)), ctypes.POINTER(ctypes.c_char)))
    blob_out = DATA_BLOB()
    if not ctypes.windll.crypt32.CryptProtectData(
            ctypes.byref(blob_in), None, None, None, None, 0,
            ctypes.byref(blob_out)):
        raise OSError("CryptProtectData failed")
    try:
        return ctypes.string_at(blob_out.pbData, blob_out.cbData)
    finally:
        ctypes.windll.kernel32.LocalFree(blob_out.pbData)


def _unprotect(data: bytes) -> bytes:
    import ctypes
    import ctypes.wintypes as wt

    class DATA_BLOB(ctypes.Structure):
        _fields_ = [("cbData", wt.DWORD), ("pbData",
                                            ctypes.POINTER(ctypes.c_char))]

    blob_in = DATA_BLOB(len(data), ctypes.cast(ctypes.create_string_buffer(
        data, len(data)), ctypes.POINTER(ctypes.c_char)))
    blob_out = DATA_BLOB()
    if not ctypes.windll.crypt32.CryptUnprotectData(
            ctypes.byref(blob_in), None, None, None, None, 0,
            ctypes.byref(blob_out)):
        raise OSError("CryptUnprotectData failed")
    try:
        return ctypes.string_at(blob_out.pbData, blob_out.cbData)
    finally:
        ctypes.windll.kernel32.LocalFree(blob_out.pbData)


class SecretStore:
    def __init__(self, data_dir: str | Path) -> None:
        self.dir = Path(data_dir).expanduser()
        self.dir.mkdir(parents=True, exist_ok=True)
        self.dpapi = _dpapi_available()
        if not self.dpapi:
            log.warning("secrets: DPAPI unavailable on %s — plaintext 0600 "
                        "fallback (development only)", sys.platform)

    def _path(self, broker: str) -> Path:
        safe = "".join(c for c in broker if c.isalnum() or c in "-_")
        return self.dir / f"secrets-{safe}.{'bin' if self.dpapi else 'json'}"

    def save(self, broker: str, secrets: dict) -> None:
        payload = json.dumps(secrets).encode()
        p = self._path(broker)
        if self.dpapi:
            p.write_bytes(_protect(payload))
        else:
            p.write_text(json.dumps(secrets), encoding="utf-8")
            os.chmod(p, 0o600)

    def load(self, broker: str) -> dict | None:
        p = self._path(broker)
        if not p.exists():
            return None
        try:
            raw = _unprotect(p.read_bytes()) if self.dpapi \
                else p.read_text(encoding="utf-8").encode()
            return json.loads(raw.decode())
        except Exception as e:
            log.error("secrets: cannot read %s: %s", p, e)
            return None

    def clear(self, broker: str) -> None:
        p = self._path(broker)
        if p.exists():
            p.unlink()

    @property
    def backend(self) -> str:
        return "DPAPI" if self.dpapi else "plaintext-0600 (dev only)"
