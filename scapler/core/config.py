"""Settings loader (plan.md §8). Secrets are NOT here — DPAPI store, Phase 6."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path


@dataclass(frozen=True)
class Targets:
    t1: float = 5.0
    t2: float = 12.0
    t3: float = 20.0
    sl: float = 8.0


@dataclass(frozen=True)
class Setup:
    rsi_ce: tuple[float, float] = (55.0, 78.0)
    rsi_pe: tuple[float, float] = (22.0, 45.0)
    vol_mult: float = 1.5
    atr_min: dict[str, float] = field(default_factory=lambda: {
        "NIFTY": 10.0, "BANKNIFTY": 25.0, "SENSEX": 30.0,
        "FINNIFTY": 12.0, "MIDCPNIFTY": 15.0,
    })
    delta_band: tuple[float, float] = (0.45, 0.60)
    max_spread_ticks: int = 2


@dataclass(frozen=True)
class Settings:
    broker_active: str = "upstox"
    steps: dict[str, float] = field(default_factory=lambda: {
        "NIFTY": 50.0, "BANKNIFTY": 100.0, "SENSEX": 100.0,
        "FINNIFTY": 50.0, "MIDCPNIFTY": 25.0,
    })
    index: str = "BANKNIFTY"
    lot_multiplier: int = 1
    mode: str = "MANUAL"                      # AUTO | MANUAL
    targets: Targets = field(default_factory=Targets)
    trail_n1: tuple[str, str] = ("BE", "T1")  # N=1 lot ladder after T1, T2
    time_stop_candles: int = 15
    signal_ttl_candles: int = 3               # 0 = hold SIGNALED until break
    entry_window: tuple[str, str] = ("09:17", "15:15")
    square_off: str = "15:20"
    max_trades_per_day: int = 3
    max_daily_loss_inr: float = 2500.0
    sl_streak_stop: int = 2
    stale_feed_s: float = 5.0
    reconnect_stale_s: float = 30.0
    orphan_policy: str = "square_off"         # square_off | alert_only
    setup: Setup = field(default_factory=Setup)
    recenter_steps: int = 2
    replay: bool = False


_TUPLE_FIELDS = {"trail_n1", "entry_window"}
_NESTED = {"targets": Targets, "setup": Setup}
_NESTED_TUPLE_FIELDS = {"rsi_ce", "rsi_pe", "delta_band"}


def _build(cls, raw: dict, tuple_fields: set[str]):
    kwargs = {}
    for k, v in raw.items():
        if k in tuple_fields and isinstance(v, list):
            v = tuple(v)
        kwargs[k] = v
    return cls(**kwargs)


def load(path: str | Path | None = None) -> Settings:
    if path is None or not Path(path).exists():
        return Settings()
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    nested = {}
    for key, cls in _NESTED.items():
        if key in raw and isinstance(raw[key], dict):
            nested[key] = _build(cls, raw.pop(key), _NESTED_TUPLE_FIELDS)
    s = _build(Settings, raw, _TUPLE_FIELDS)
    for k, v in nested.items():
        s = replace(s, **{k: v})
    return s


def save(settings: Settings, path: str | Path) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(asdict(settings), indent=2, sort_keys=True),
                 encoding="utf-8")
