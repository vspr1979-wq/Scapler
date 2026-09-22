"""Incremental indicator engine (plan §5-F7). O(1) per closed candle.

Definitions (all on the SELECTED INDEX SPOT 1-min candles):
  VWAP   session Σ(typical·vol)/Σvol, typical=(H+L+C)/3, vol = candle volume
         (CandleBuilder supplies feed volume where available, else a
          tick-activity proxy — see agents/candle.py)
  EMA    9/21, seeded with first close (identical to pandas ewm(span,
         adjust=False))
  RSI14  Wilder: SMA seed over first 14 changes, then (prev*13+x)/14;
         warm-up (<15 candles) reports 50.0 = neutral → entry bands reject it
  ATR14  Wilder on TR; 0.0 until seeded → ATR gate rejects warm-up
  vol×   candle volume / mean(previous ≤20 candle volumes); 0.0 on first bar
No value is ever invented: warm-up outputs are explicit neutral constants that
the entry gates reject, never guessed market numbers.
"""
from __future__ import annotations

from collections import deque

from ..core.messages import CandleClosed, IndicatorsReady


class IndicatorEngine:
    def __init__(self, key: str, ema_fast: int = 9, ema_slow: int = 21,
                 rsi_period: int = 14, atr_period: int = 14,
                 vol_lookback: int = 20) -> None:
        self.key = key
        self.pf, self.ps = ema_fast, ema_slow
        self.rp, self.ap = rsi_period, atr_period
        self.vl = vol_lookback
        self.reset_session()

    def reset_session(self) -> None:
        self.n = 0
        self._vwap_pv = 0.0
        self._vwap_v = 0.0
        self.ema9 = 0.0
        self.ema21 = 0.0
        self._prev_close: float | None = None
        self._gains: list[float] = []
        self._losses: list[float] = []
        self._avg_g = 0.0
        self._avg_l = 0.0
        self._trs: list[float] = []
        self._atr = 0.0
        self._vols: deque[float] = deque(maxlen=self.vl + 1)
        self.rsi = 50.0
        self.atr = 0.0
        self.vwap = 0.0
        self.vol_ratio = 0.0

    @property
    def warmed_up(self) -> bool:
        return self.n >= self.rp + 1 and self.n >= self.ap + 1

    def on_candle(self, c: CandleClosed) -> IndicatorsReady:
        self.n += 1
        v = c.volume if c.volume > 0 else 1.0
        tp = (c.h + c.l + c.c) / 3.0
        self._vwap_pv += tp * v
        self._vwap_v += v
        self.vwap = self._vwap_pv / self._vwap_v

        if self.n == 1:
            self.ema9 = self.ema21 = c.c
        else:
            kf, ks = 2.0 / (self.pf + 1), 2.0 / (self.ps + 1)
            self.ema9 += (c.c - self.ema9) * kf
            self.ema21 += (c.c - self.ema21) * ks

        # RSI (Wilder)
        if self._prev_close is not None:
            ch = c.c - self._prev_close
            g, l = max(ch, 0.0), max(-ch, 0.0)
            if len(self._gains) < self.rp:
                self._gains.append(g)
                self._losses.append(l)
                if len(self._gains) == self.rp:
                    self._avg_g = sum(self._gains) / self.rp
                    self._avg_l = sum(self._losses) / self.rp
            else:
                self._avg_g = (self._avg_g * (self.rp - 1) + g) / self.rp
                self._avg_l = (self._avg_l * (self.rp - 1) + l) / self.rp
            if len(self._gains) == self.rp:
                if self._avg_l == 0.0:
                    self.rsi = 100.0 if self._avg_g > 0.0 else 50.0
                else:
                    self.rsi = 100.0 - 100.0 / (1.0 + self._avg_g / self._avg_l)

        # ATR (Wilder)
        tr = (c.h - c.l) if self._prev_close is None else max(
            c.h - c.l, abs(c.h - self._prev_close), abs(c.l - self._prev_close))
        if len(self._trs) < self.ap:
            self._trs.append(tr)
            if len(self._trs) == self.ap:
                self._atr = sum(self._trs) / self.ap
        else:
            self._atr = (self._atr * (self.ap - 1) + tr) / self.ap
        self.atr = self._atr if len(self._trs) == self.ap else 0.0

        # volume ratio vs previous bars
        prev = list(self._vols)
        self.vol_ratio = (v / (sum(prev) / len(prev))) if prev else 0.0
        self._vols.append(v)

        self._prev_close = c.c
        return IndicatorsReady(key=c.key, candle_close_ts=c.close_ts,
                               vwap=self.vwap, ema9=self.ema9, ema21=self.ema21,
                               rsi=self.rsi, atr=self.atr,
                               vol_ratio=self.vol_ratio)
