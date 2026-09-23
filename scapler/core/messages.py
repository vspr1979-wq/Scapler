"""Canonical message catalog (immutable msgspec structs) + topic registry.

Rules (plan.md §3.2):
  * every message is frozen → no locks, no defensive copies
  * ``OrderRequest`` carries an ``OrderIntent``, never a raw side → sell-to-open
    is unrepresentable (SideGuard, plan §5-F2)
  * hot topics are coalesced latest-wins per key; order/kill topics are priority
"""
from __future__ import annotations

import enum
from typing import Any

import msgspec


# ────────────────────────────── topics ──────────────────────────────
class Topic:
    TICK_RAW = "tick.raw"
    CANDLE_CLOSED = "candle.closed"
    INDICATORS_READY = "indicators.ready"
    SIGNAL_NEW = "signal.new"
    SIGNAL_STATE = "signal.state"
    SIGNAL_EXECUTE = "signal.execute"      # side payload; AUTO or UI click
    UI_EXECUTE = "ui.execute"              # MANUAL click from the UI tab
    STRIKE_SELECTED = "strike.selected"
    WINDOW_REBUILT = "window.rebuilt"
    ORDER_REQUEST = "order.request"
    ORDER_APPROVED = "order.approved"
    RISK_VETO = "risk.veto"
    ORDER_REQ = "order.req"
    ORDER_FILL = "order.fill"
    ORDER_REJECTED = "order.rejected"
    POSITION_UPDATE = "position.update"
    EXIT_TRIGGER = "exit.trigger"
    KILL_SWITCH = "kill.switch"
    AGENT_HEALTH = "agent.health"
    UI_SNAPSHOT = "ui.snapshot"
    FEED_RECONNECT = "feed.reconnect"        # watchdog/UI → MarketData
    WATCHDOG_STATUS = "watchdog.status"      # watchdog → UI (1 Hz dict)
    CONNECTION_STATUS = "connection.status"  # connection → UI (dict)
    SESSION_NEW_DAY = "session.new_day"      # watchdog → risk/journal/UI


PRIORITY_TOPICS = frozenset({
    Topic.ORDER_REQUEST, Topic.ORDER_APPROVED, Topic.RISK_VETO,
    Topic.ORDER_REQ, Topic.ORDER_FILL, Topic.ORDER_REJECTED,
    Topic.EXIT_TRIGGER, Topic.KILL_SWITCH,
})

COALESCED_TOPICS = frozenset({Topic.TICK_RAW, Topic.UI_SNAPSHOT})

WAKE_TOPIC = "__wake__"  # internal inbox sentinel, never published on the bus


# ────────────────────────────── enums ──────────────────────────────
class OrderIntent(enum.Enum):
    BUY_TO_OPEN = "BUY_TO_OPEN"      # long premium only
    SELL_TO_CLOSE = "SELL_TO_CLOSE"  # close an existing long only


class OptionType(enum.Enum):
    CE = "CE"
    PE = "PE"


class SignalStateEnum(enum.Enum):
    DISARMED = "DISARMED"
    ARMED = "ARMED"
    SIGNALED = "SIGNALED"
    HELD = "HELD"


class ExitReason(enum.Enum):
    T1 = "T1"
    T2 = "T2"
    T3 = "T3"
    SL = "SL"
    TIME_STOP = "TIME_STOP"
    SQUARE_OFF = "SQUARE_OFF"
    KILL = "KILL"
    MANUAL = "MANUAL"


# ────────────────────────────── messages ──────────────────────────────
class InstrumentKey(msgspec.Struct, frozen=True):
    exchange: str                     # NSE_FO | BSE_FO | NSE_INDEX | BSE_INDEX
    token: str
    symbol: str                       # broker trading symbol
    strike: float = 0.0
    expiry: str = ""                  # ISO date, from instrument master
    option_type: OptionType | None = None

    @property
    def feed_key(self) -> str:
        return f"{self.exchange}|{self.token}"


class Tick(msgspec.Struct, frozen=True):
    key: str                          # InstrumentKey.feed_key
    exch_ts_ns: int
    ltp: float
    bid: float = 0.0
    ask: float = 0.0
    oi: float = 0.0
    volume: float = 0.0
    delta: float | None = None
    gamma: float | None = None


class CandleClosed(msgspec.Struct, frozen=True):
    key: str                          # selected index spot feed key
    tf: str                           # "1m"
    open_ts: int
    close_ts: int
    o: float
    h: float
    l: float
    c: float
    volume: float


class IndicatorsReady(msgspec.Struct, frozen=True):
    key: str
    candle_close_ts: int
    vwap: float
    ema9: float
    ema21: float
    rsi: float
    atr: float
    vol_ratio: float


class SignalState(msgspec.Struct, frozen=True):
    side: OptionType
    old: SignalStateEnum
    new: SignalStateEnum
    reason: str
    candle_ts: int = 0
    ttl_candles: int = 0
    tick_to_signal_ms: float = 0.0


class StrikeSelected(msgspec.Struct, frozen=True):
    side: OptionType
    instrument: InstrumentKey
    delta: float | None
    spread_ticks: int
    window_lo: float
    window_hi: float


class OrderRequest(msgspec.Struct, frozen=True):
    intent: OrderIntent               # NO raw side field exists
    instrument: InstrumentKey
    qty: int                          # whole lots only
    client_id: str                    # idempotency key (8-20 alnum)
    ts_mono: int


class OrderFill(msgspec.Struct, frozen=True):
    client_id: str
    instrument: InstrumentKey
    intent: OrderIntent
    qty: int
    price: float
    latency_ms: float
    ts_mono: int
    lot_size: int = 0                      # carried for the exit ladder


class PositionUpdate(msgspec.Struct, frozen=True):
    feed_key: str
    qty_open: int
    avg_price: float
    closed: bool
    exit_reason: str = ""
    realized_pnl: float = 0.0
    t1_hit: bool = False               # ladder state for the UI position panel
    t2_hit: bool = False
    sl_price: float = 0.0              # live (trailed) stop premium


class OrderRejected(msgspec.Struct, frozen=True):
    client_id: str
    reason: str


class RiskVeto(msgspec.Struct, frozen=True):
    code: str                         # V_WINDOW, V_MAXTRADES, ... , SG_*
    context: str = ""


class ExitTrigger(msgspec.Struct, frozen=True):
    reason: ExitReason
    instrument: InstrumentKey
    qty: int
    ref_price: float                  # SL/target reference premium


class KillSwitch(msgspec.Struct, frozen=True):
    source: str                       # "ui" | "hotkey" | "watchdog"


class AgentHealth(msgspec.Struct, frozen=True):
    name: str
    state: str
    restarts: int
    inbox_depth: int
    p99_ms: float
    beat_age_s: float


class Envelope(msgspec.Struct, frozen=True):
    seq: int
    topic: str
    key: str | None
    ts_mono: int                      # publish stamp (monotonic ns)
    payload: Any
