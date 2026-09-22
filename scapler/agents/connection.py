"""ConnectionAgent — login/token lifecycle + master acquisition (plan §5-F3).

Owns broker session state for BOTH brokers (one ACTIVE at a time — the
runtime enforces flat-before-switch). Flow per connect:
  secrets (SecretStore) → adapter construct → login() (Groww: key/secret
  [→TOTP]; Upstox: OAuth access-code paste) → master via memory→disk→network
  cache → publish ``connection.status``. Token auto-refresh window
  09:10–09:16 IST daily (task loop).

Network is only ever touched here and in the adapters — never in the UI or
the trading agents. Unit tests stub the HTTP layer; live connects run on
the operator machine.
"""
from __future__ import annotations

import asyncio
import logging

from ..brokers.master_cache import get_master
from ..core.clock import ist_hhmm
from ..core.config import Settings
from ..core.messages import Topic
from ..core.secrets_store import SecretStore
from .base_imports import Agent

log = logging.getLogger(__name__)


class ConnectionAgent(Agent):
    name = "connection"
    topics = (Topic.KILL_SWITCH,)

    def __init__(self, bus, settings: Settings, store: SecretStore,
                 clock_fn=None, adapter_factory=None) -> None:
        super().__init__(bus)
        self.cfg = settings
        self.store = store
        self._clock = clock_fn or ist_hhmm
        # adapter_factory(broker, creds_dict) -> BrokerAdapter; injectable
        # for tests, defaults to the real Groww/Upstox classes.
        self._factory = adapter_factory or _default_factory
        self.sessions: dict[str, dict] = {
            "upstox": {"state": "DISCONNECTED", "error": "", "adapter": None},
            "groww": {"state": "DISCONNECTED", "error": "", "adapter": None},
        }
        self.active: str | None = None
        self.master = None

    # ── public API (runtime calls these on UI commands) ─────────────
    async def save_secrets(self, broker: str, secrets: dict) -> dict:
        cleaned = {k: v for k, v in secrets.items() if v}
        if not cleaned:
            return {"ok": False, "error": "nothing to save"}
        self.store.save(broker, cleaned)
        self._publish_status(broker, note=f"saved ({self.store.backend})")
        return {"ok": True, "note": f"secrets saved — {self.store.backend}"}

    async def connect(self, broker: str, extra: dict | None = None) -> dict:
        extra = extra or {}
        sess = self.sessions.get(broker)
        if sess is None:
            return {"ok": False, "error": f"unknown broker {broker}"}
        if sess["state"] == "CONNECTED":
            return {"ok": True, "note": f"{broker} already connected"}
        secrets = self.store.load(broker) or {}
        secrets.update({k: v for k, v in extra.items() if v})
        if not secrets.get("api_key") or not (
                secrets.get("api_secret") or extra.get("code")):
            err = "missing credentials — save API key/secret first" \
                if broker == "groww" else \
                "missing credentials — save API key/secret, then paste the " \
                "OAuth access code"
            self._set(broker, "ERROR", err)
            return {"ok": False, "error": err}
        self._set(broker, "CONNECTING")
        try:
            adapter = self._factory(broker, secrets)
            await adapter.login(extra.get("code", ""))
            self.sessions[broker]["adapter"] = adapter
            self.master = await get_master(adapter, self.cfg.data_dir)
            self.active = broker
            self._set(broker, "CONNECTED",
                      master_source=self.master.source,
                      master_options=len(self.master.options))
            return {"ok": True,
                    "note": f"{broker} connected · master "
                            f"{len(self.master.options)} options "
                            f"({self.master.source})"}
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            log.error("connect %s failed: %s", broker, err)
            self._set(broker, "ERROR", err)
            return {"ok": False, "error": err}

    async def disconnect(self, broker: str) -> dict:
        sess = self.sessions.get(broker)
        if sess and sess["adapter"] is not None:
            try:
                await sess["adapter"].disconnect()
            except Exception as e:
                log.warning("disconnect %s: %s", broker, e)
            sess["adapter"] = None
        if self.active == broker:
            self.active = None
        self._set(broker, "DISCONNECTED")
        return {"ok": True}

    def adapter(self, broker: str | None = None):
        b = broker or self.active
        return self.sessions.get(b, {}).get("adapter") if b else None

    async def positions(self) -> list:
        ad = self.adapter()
        if ad is None:
            return []
        try:
            return await ad.positions()
        except Exception as e:
            log.warning("positions fetch failed: %s", e)
            return []

    # ── internals ───────────────────────────────────────────────────
    def _set(self, broker: str, state: str, error: str = "", **extra) -> None:
        self.sessions[broker].update(state=state, error=error)
        self._publish_status(broker, **extra)

    def _publish_status(self, broker: str, note: str = "", **extra) -> None:
        sess = self.sessions[broker]
        self.publish(Topic.CONNECTION_STATUS, {
            "broker": broker, "state": sess["state"], "error": sess["error"],
            "active": self.active, "note": note,
            "master_source": extra.get("master_source", ""),
            "master_options": extra.get("master_options", 0),
            "secrets_backend": self.store.backend})

    async def on_start(self) -> None:
        self._refresh = asyncio.get_running_loop().create_task(
            self._refresh_loop())

    async def _refresh_loop(self) -> None:
        """Token refresh window 09:10–09:16 IST (plan §14): re-login the
        active broker once per day inside the window."""
        done_day = ""
        try:
            while True:
                await asyncio.sleep(20.0)
                hhmm = self._clock()
                from ..core.clock import ist_now
                today = ist_now().date().isoformat()
                if self.active and "09:10" <= hhmm <= "09:16" \
                        and done_day != today:
                    done_day = today
                    ad = self.adapter()
                    if ad is not None:
                        try:
                            await ad.login("")
                            self._publish_status(self.active,
                                                 note="token refreshed")
                        except Exception as e:
                            self._set(self.active, "ERROR",
                                      f"refresh failed: {e}")
        except asyncio.CancelledError:
            raise

    async def on_message(self, env) -> None:
        pass                                  # kill-switch: nothing to close

    async def on_stop(self) -> None:
        if getattr(self, "_refresh", None) is not None:
            self._refresh.cancel()
            try:
                await self._refresh
            except asyncio.CancelledError:
                pass


def _default_factory(broker: str, secrets: dict):
    if broker == "groww":
        from ..brokers.groww.adapter import GrowwAdapter
        from ..brokers.groww.rest import GrowwCreds
        return GrowwAdapter(GrowwCreds(
            api_key=secrets.get("api_key", ""),
            api_secret=secrets.get("api_secret", ""),
            totp_secret=secrets.get("totp_secret", "")))
    if broker == "upstox":
        from ..brokers.upstox.adapter import UpstoxAdapter
        from ..brokers.upstox.rest import UpstoxCreds
        return UpstoxAdapter(UpstoxCreds(
            api_key=secrets.get("api_key", ""),
            api_secret=secrets.get("api_secret", ""),
            redirect_uri=secrets.get("redirect_uri",
                                     "https://localhost/oauth")))
    raise ValueError(f"unknown broker {broker}")
