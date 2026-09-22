# Scapler

Windows 11 desktop algo scalper for **high-frequency Indian index OPTIONS BUYING
(long premium only)** on Upstox (API v2 + Market Data Streamer v3) and Groww (GrowwAPI).
Live scalp of 5–30 premium points on NIFTY / BANKNIFTY / SENSEX / FINNIFTY / MIDCPNIFTY
with real MARKET orders driven by closed 1-minute technicals on real exchange feeds.

**Read [`plan.md`](plan.md) first** — full design: 13-agent asyncio/EventBus
architecture, broker adapters, SideGuard (long-premium-only), 11-strike window
(5 ITM + ATM + 5 OTM), no-candle-spam signal state machines, T1/T2/T3/SL exit
ladder, risk guardrails, journal DDL, roadmap.

**Layout mock:** open [`mock.html`](mock.html) in a browser (static, no live data).
Reference mock: `ui_mockup.html` / `ui_mockup.png`.

## Status
**Phase 0 + Phase 1 complete** (2026-09-22):
- Phase 0 — core runtime (EventBus with priority lane + latest-wins coalescing,
  supervised Agent base, message catalog, settings) and the pure strategy core:
  SideGuard (F2), strike window (F5), no-spam signal FSMs (F6), T1/T2/T3/SL exit
  ladder (§6). Bus bench: 565k msg/s, p99 0.17 ms.
- Phase 1 — Upstox adapter: REST v2 (OAuth/orders/positions), instrument master
  parser, Streamer v3 protobuf decoder (official vendored schema), feed handle,
  tick recorder/replayer.
89 unit tests green. Next: Phase 2 Groww adapter, then Phase 3 candle/indicator
agents, Phase 4 order/risk/position agents, Phase 5 UI.

## Dev bootstrap
```bash
./tools/bootstrap.sh          # installs deps (msgspec, orjson, pytest, ...) + runs unit tests
python3 -m pytest tests/unit -q
python3 tests/bench/bench_bus.py
```

## Hard rules (never violated, enforced by SideGuard)
- Entry = BUY Call / BUY Put only. Exit = SELL to close that long only.
- No sell-to-open, no shorts, no futures, no spreads, no straddles.
- No invented market data; no signal spam (one signal per side per setup episode).
