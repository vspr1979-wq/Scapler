"""No-candle-spam signal state machines (plan.md §5-F6).

Per side, independently:

    DISARMED ──break candle──► ARMED ──valid candle──► SIGNALED
        ▲                                                  │
        ├── break / TTL expiry ────────────────────────────┤
        └── position flat ── HELD ◄── executed ────────────┘

While SIGNALED nothing repeats on later candles — the signal fires once per
setup episode, in AUTO and MANUAL alike. Re-arm requires the setup to BREAK
on a closed candle and then form again.
"""
from __future__ import annotations

from ..core.config import Settings
from ..core.messages import OptionType, SignalState, SignalStateEnum as S


class SideFSM:
    def __init__(self, side: OptionType, settings: Settings) -> None:
        self.side = side
        self.ttl = settings.signal_ttl_candles
        self.state = S.ARMED          # session start: nothing to break yet
        self.ttl_left = 0

    def _go(self, new: S, reason: str, candle_ts: int, tick_ms: float = 0.0) -> SignalState:
        old, self.state = self.state, new
        return SignalState(side=self.side, old=old, new=new, reason=reason,
                           candle_ts=candle_ts, ttl_candles=self.ttl_left,
                           tick_to_signal_ms=tick_ms)

    def on_candle(self, valid: bool, broken: bool, held: bool,
                  candle_ts: int, tick_ms: float = 0.0) -> list[SignalState]:
        out: list[SignalState] = []
        if held and self.state is not S.HELD:
            out.append(self._go(S.HELD, "position open", candle_ts))
            return out
        if self.state is S.HELD:
            return out                       # flat is pushed via set_held(False)
        if self.state is S.DISARMED:
            if broken:
                out.append(self._go(S.ARMED, "setup broke on closed candle", candle_ts))
            return out                       # valid candles here are IGNORED
        if self.state is S.ARMED:
            if valid:
                self.ttl_left = self.ttl
                out.append(self._go(S.SIGNALED, "setup valid on closed candle",
                                    candle_ts, tick_ms))
            return out
        # SIGNALED — never repeat while the setup lives
        if broken:
            out.append(self._go(S.DISARMED, "setup broke on closed candle", candle_ts))
        elif self.ttl > 0:
            self.ttl_left -= 1
            if self.ttl_left == 0:
                out.append(self._go(S.DISARMED, "signal ttl expired", candle_ts))
        return out

    def set_held(self, held: bool, candle_ts: int = 0) -> list[SignalState]:
        if held and self.state is not S.HELD:
            return [self._go(S.HELD, "position open", candle_ts)]
        if not held and self.state is S.HELD:
            return [self._go(S.DISARMED, "position flat — break required to re-arm",
                             candle_ts)]
        return []


class SignalEngine:
    """CE + PE machines + the v1 single-position HELD coupling."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.fsm = {s: SideFSM(s, settings) for s in OptionType}
        self.held = False

    def on_candle(self, candle_ts: int, valid: dict[OptionType, bool],
                  broken: dict[OptionType, bool],
                  tick_ms: float = 0.0) -> list[SignalState]:
        out: list[SignalState] = []
        for side in OptionType:
            out += self.fsm[side].on_candle(valid[side], broken[side],
                                            self.held, candle_ts, tick_ms)
        return out

    def set_held(self, held: bool, candle_ts: int = 0) -> list[SignalState]:
        self.held = held
        out: list[SignalState] = []
        for side in OptionType:
            out += self.fsm[side].set_held(held, candle_ts)
        return out

    def signaled_side(self) -> OptionType | None:
        for side in OptionType:
            if self.fsm[side].state is S.SIGNALED:
                return side
        return None
