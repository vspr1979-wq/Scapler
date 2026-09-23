# SCAPLER — Build Plan & Application Design

**Status:** `DRAFT — AWAITING LAYOUT CONFIRMATION` (see `mock.html` + §15 Open Questions)
**Version:** 1.0-draft · **Date:** 2026-09-22 · **Branch:** `arena/01a0c9cc-scapler`
**Companion artifact:** [`mock.html`](mock.html) — static, self-contained UI layout mock (open it in a browser; **no live data, no trading logic**). The earlier reference mock is kept as `ui_mockup.html` / `ui_mockup.png`.

> **Rule of this document:** nothing gets built until the layout + open questions in §15 are confirmed.
> Every number marked *(default)* is a Settings-driven constant, not a hardcode.

---

## 0. Product definition

### 0.1 Mission
Windows 11 desktop algo scalper for **high-frequency Indian index OPTIONS BUYING (long premium only)** on **Upstox (API v2 + Market Data Streamer v3)** and **Groww (GrowwAPI)**.
Live scalp of **5–30 points of option premium** on NIFTY / BANKNIFTY / SENSEX / FINNIFTY / MIDCPNIFTY, using **real MARKET orders only**, driven exclusively by **closed 1-minute technicals on real exchange feeds**.

### 0.2 Hard rules — OPTIONS BUYING ONLY (non-negotiable, enforced in code by `SideGuard`, §6-F2)
| # | Rule |
|---|------|
| 1 | Entry is always **BUY** Call or **BUY** Put (buy-to-open). |
| 2 | Exit is always **SELL** to close that long (sell-to-close). |
| 3 | Never sell-to-open, never short options, never futures, never spreads, never straddles. |
| 4 | "BUY PE" = buying a put. It never means selling a call. |
| 5 | T1 / T2 / T3 / SL exits never reverse into a short; residual qty after partials is only ever sold-to-close. |

### 0.3 Non-goals (v1)
No stocks · no short options · no multi-leg spreads · no backtester · no paper-fill simulator (unless explicitly added later) · **no chain scan beyond 5 ITM + ATM + 5 OTM** · no trading five indices at once · **no invented/mocked market data in live mode** · **no new signal on every candle while the same setup is still true** · no charts (text-only UI, §8).

> **Replay note:** a *recorder* captures real live ticks to SQLite for offline regression tests. Replay is labelled `REPLAY` in the UI/banner and can never arm the Signal agent in live mode. Recorded real data ≠ invented data.

---

## 1. Instruments & contract specification

| Index | Exchange | Strike step *(default)* | Lot size *(default)* | Option segment |
|---|---|---|---|---|
| NIFTY | NSE | 50 | 65 | NSE_FO |
| BANKNIFTY | NSE | 100 | 30 | NSE_FO |
| SENSEX | BSE | 100 | 20 | BSE_FO |
| FINNIFTY | NSE | 50 | 60 | NSE_FO |
| MIDCPNIFTY | NSE | 25 | 120 | NSE_FO |

- **Expiry:** nearest *live* expiry parsed from the exchange instrument master at connect time (`StrikeAgent`). **No hardcoded weekday.** Re-resolved daily and on master refresh; if the nearest expiry rolls (weekly → next weekly), the strike window is rebuilt and old subscriptions are dropped.
- **Lot multiplier:** 1–10, default 1. Order qty = `lots × lot_size` (whole lots only, §7).
- **Lot/step validation:** table values are defaults for display & sanity checks; the instrument master is the runtime source of truth. Mismatch master vs table → warn in Journal + status bar, use master.
- **One index traded at a time.** Header strip always shows **NIFTY, BANKNIFTY, SENSEX** LTPs even when FINNIFTY/MIDCPNIFTY is selected (§8).

---

## 2. Tech stack

| Layer | Choice | Why |
|---|---|---|
| Language | Python 3.12 | asyncio-native, broker SDKs exist |
| Event loop | `winloop` (uvloop port for Windows) | lowest-latency loop on Win11 |
| UI shell | `pywebview` (Edge **WebView2**, shipped with Win11) + vanilla HTML/CSS/JS | Qt dropped: ~15 MB installer, <120 MB RAM, text-only panels render fast |
| UI transport | pywebview `js_api` (JS→Py commands) + `evaluate_js` snapshots (Py→UI, coalesced 10–20 Hz) | no local HTTP server, no CORS/origin issues |
| Serialization | `msgspec` (internal msgs), `orjson` (UI snapshots, journal payloads) | zero-copy-ish, no locks/copies on hot path |
| Upstox | REST v2 (`api.upstox.com/v2`) + **Market Data Streamer v3** (`/v3/feed/market-data-feed/authorize` → one-time `wss://` URI, protobuf `MarketDataFeedV3.FeedResponse`) | official v2/v3 |
| Groww | REST `api.groww.in/v1` (+ `growwapi` Python SDK), OAuth2 daily token, WS for live data & order updates | official GrowwAPI |
| HTTP | `aiohttp` (single pooled session per broker) | async, keep-alive |
| WS | `websockets` (client) | async, binary frames |
| Persistence | SQLite (WAL, single writer = `JournalAgent`) + CSV export | audit trail, zero-config |
| Secrets | Windows **DPAPI** via `keyring`/`pywin32` `CryptProtectData` for API secret/PIN at rest | never plaintext in settings.json |
| Packaging | PyInstaller (onedir) + Inno Setup; WebView2 presence check | Win11 target |
| Testing | `pytest` + `pytest-asyncio`, replay harness, crash-injection, p99 latency bench | §12 |

**Performance budget** (measured end-to-end, shown in status bar):
| Hop | Budget |
|---|---|
| tick receive → signal decision | < 50 ms (p99) |
| signal → order dispatch to adapter | < 10 ms |
| UI snapshot push | 10–20 Hz coalesced (never per-tick) |
| RSS footprint | < 120 MB |
| Installer size | ~15 MB |

---

## 3. Architecture — async multi-agent + in-process pub/sub Event Bus

Every job is an **independent agent**: an `asyncio.Task` with its own inbox, communicating **only** through the Event Bus. No agent imports another agent; they share immutable `msgspec` messages.

### 3.1 Agent roster (13)

| # | Agent | One job |
|---|---|---|
| 1 | `MarketDataAgent` | WS ingest (broker feed), protobuf/JSON decode, LTP cache, ticks/s + feed-latency stats |
| 2 | `CandleBuilderAgent` | ticks → **closed 1-min bars only** (selected index spot); emits `candle.closed` |
| 3 | `IndicatorAgent` | incremental VWAP, EMA9/21, RSI14, ATR14, volume-vs-20-avg; emits `indicators.ready` |
| 4 | `SignalAgent` | CE + PE independent state machines (§6-F6); emits `signal.new` / `signal.state` |
| 5 | `StrikeAgent` | instrument master, nearest expiry, 11-strike window, re-centering, strike pick, subscriptions |
| 6 | `RiskAgent` | breakers, time guards, daily counters, **veto power** over every order |
| 7 | `OrderAgent` | MARKET orders, **SideGuard**, idempotency, order-state tracking, priority lane |
| 8 | `PositionExitAgent` | T1/T2/T3/SL tick-watch, partials in whole lots, trail ladder, time-stop, 15:20 square-off |
| 9 | `ConnectionAgent` | login/token lifecycle per broker, connect/disconnect commands, token refresh |
| 10 | `WatchdogAgent` | stale-feed detection, WS reconnect with backoff, orphan-position reconciliation on restart |
| 11 | `JournalAgent` | single-writer SQLite audit (signals, orders, fills, vetoes, state changes, sessions) |
| 12 | `UIAgent` | coalesced snapshots → WebView2; JS commands → bus; tab data providers |
| 13 | `SupervisorAgent` | agent health, crash-restart with backoff, kill-switch orchestration, health table for Agents tab |

### 3.2 Event Bus — topics

| Topic | Producer | Consumers | Rate |
|---|---|---|---|
| `tick.raw` | MarketData | CandleBuilder, PositionExit, UI(LTP cache) | hot, coalesced latest-wins per key |
| `candle.closed` | CandleBuilder | Indicator | 1/min |
| `indicators.ready` | Indicator | Signal | 1/min |
| `signal.new` / `signal.state` | Signal | UI, Journal, Risk | ≤ few/day |
| `strike.selected` / `window.rebuilt` | Strike | UI, MarketData(subs), Journal | on drift/roll |
| `order.request` | Signal(auto)/UI(manual)/PositionExit | Risk | **priority lane** |
| `order.approved` / `risk.veto` | Risk | Order / UI+Journal | per request |
| `order.req` → broker | Order | adapter | per approved |
| `order.fill` / `order.rejected` | Order | PositionExit, Risk, UI, Journal | per order |
| `position.update` / `exit.trigger` | PositionExit | Order, UI, Journal | tick-driven |
| `kill.switch` | UI/any | Supervisor → all | manual |
| `agent.health` | all (heartbeat) | Supervisor, UI | 1 Hz |
| `ui.snapshot` | UIAgent | WebView2 | 10–20 Hz |

**Messaging rules for speed**
1. Fire-and-forget hot path (`bus.publish` never awaits consumers).
2. **Latest-wins coalescing** on `tick.raw` per instrument key (inbox slot, not unbounded queue → no queue bloat after a stall).
3. Immutable `msgspec.Struct` messages (frozen) → no locks, no defensive copies.
4. **Priority lane**: `order.*` and `kill.switch` jump ahead of telemetry in every inbox.
5. **Single-writer state ownership**: each state (LTP cache, candles, positions, journal) has exactly one owning agent; readers get snapshots.

### 3.3 Data-flow choreography (entry → exit)

```
 1 MarketData  WS frame → decode → tick.raw (coalesced)         [t0]
 2             LTP cache update → header/strike-window LTPs → ui.snapshot (10–20 Hz)
 3 CandleBuilder  spot tick → bar; on bar close → candle.closed  [t0+Δ ≤ 1 ms]
 4 Indicator   incremental VWAP/EMA/RSI/ATR/vol → indicators.ready
 5 Signal      CE & PE state machines on the CLOSED candle → signal.new (or nothing)
 6 Strike      window center check (±2 steps drift → rebuild); pick strike (delta band, liquidity)
 7 Risk        veto checks → order.approved | risk.veto(reason)
 8 Order       SideGuard → BUY_TO_OPEN MARKET → adapter            [< 10 ms dispatch]
 9 Order       fill → order.fill → PositionExit arms T1/T2/T3/SL
10 PositionExit option-LTP tick-watch → exit.trigger → SELL_TO_CLOSE → flat → machines DISARMED
```

Each hop stamps `time.monotonic_ns()`; `tick→signal` and `signal→dispatch` p50/p99 are shown in the status bar and journalled per trade.

---

## 4. Broker adapters

### 4.1 Adapter ABC (makes illegal orders unrepresentable)

```python
class OrderIntent(enum.Enum):
    BUY_TO_OPEN  = "BUY_TO_OPEN"    # long premium only
    SELL_TO_CLOSE = "SELL_TO_CLOSE" # close an existing long only

@dataclass(frozen=True)
class OrderRequest:
    intent: OrderIntent          # NO raw "side" field exists → sell-to-open is unrepresentable
    instrument: InstrumentKey    # exchange, token, symbol, strike, expiry, option_type
    qty: int                     # whole lots only
    client_id: str               # idempotency key (8–20 alnum, satisfies Groww order_reference_id)

class BrokerAdapter(abc.ABC):
    async def login(self, creds: Secrets) -> Session: ...
    async def load_instrument_master(self) -> MasterTable: ...   # expiries, lots, steps, tokens
    async def subscribe(self, keys: list[str], mode: str) -> None: ...
    async def unsubscribe(self, keys: list[str]) -> None: ...
    async def place_market_order(self, req: OrderRequest) -> OrderAck: ...
    async def order_status(self, id_: str) -> OrderState: ...
    async def positions(self) -> list[Position]: ...
    async def cancel_all_and_square_off(self) -> None: ...
    async def disconnect(self) -> None: ...
```

Canonical normalization layer (`brokers/normalize.py`) maps broker-specific symbols/tokens/greeks/OI onto `InstrumentKey`, `Tick(exch_ts, ltp, bid, ask, oi, volume, greeks?)`, `OrderState`.

### 4.2 Upstox (API v2 + Market Data Streamer v3)
- **Auth:** OAuth2 (API key + secret + redirect URI + PIN) → daily `access_token` (`POST /v2/login/authorization/token`); stored in memory + DPAPI-backed cache; `ConnectionAgent` refreshes pre-market.
- **Feed:** `GET /v3/feed/market-data-feed/authorize` (Bearer) → `data.authorized_redirect_uri` = **one-time-use** `wss://…market-data-feeder/v3/…` URL. Connect with `websockets`, send subscribe JSON `{"guid":…, "method":"sub", "data":{"mode":"full","instrumentKeys":[…]}}`, decode protobuf `com.upstox.marketdatafeederv3udapi.rpc.proto.FeedResponse`.
  - Implementation note (known SDK pitfall): fetch the authorize URI via REST first and open the socket directly — avoids the 307-redirect handshake problem some WS clients hit.
  - Modes: `ltpc` for header indices (cheap), `full` for the 22 window options (bid/ask, OI, volume, greeks where provided).
- **Keys:** indices `NSE_INDEX|NIFTY 50`, `NSE_INDEX|Nifty Bank`, `BSE_INDEX|SENSEX`, `NSE_INDEX|NIFTY FIN SERVICE`, `NSE_INDEX|NIFTY MIDCAP SELECT`; options `NSE_FO|<token>` / `BSE_FO|<token>` from the daily instrument-master CSV.
- **Orders:** `POST /v2/order/place` — `quantity`, `product=MIS`, `validity=DAY`, `order_type=MARKET`, `transaction_type=BUY|SELL`, `tag=scapler`; rate limits respected by OrderAgent token bucket.

### 4.3 Groww (GrowwAPI) — endpoints verified 2026-09 against official docs
- **Auth:** `POST /v1/token/api/access` with `{api_key, secret}` (key+secret flow,
  daily approval) or `{api_key, totp}` (TOTP flow, no expiry); 150 req/24 h →
  `ConnectionAgent` caches the token. No redirect flow: keys come from the
  API console (`auth_url()` returns the console page).
- **Orders:** `POST /v1/order/create` (`segment=FNO`, `product=MIS`,
  `order_type=MARKET`, `order_reference_id=<client_id>` 8–20 alnum ≤2 hyphens =
  our idempotency key); cancel `POST /v1/order/cancel {segment, groww_order_id}`;
  status by id or reference id. Rate limits: orders 10/s & 250/min; live data
  10/s & 300/min → OrderAgent/MarketDataAgent token buckets.
- **Master:** `growwapi-assets.groww.in/instruments/instrument.csv`
  (exchange_token, trading_symbol, instrument_type CE/PE, underlying_symbol,
  expiry ISO, lot_size, tick_size, buy_allowed) → `MasterTable`.
- **Feed:** official `growwapi.GrowwFeed` WS client is the preferred transport
  on Windows; the implemented dependency-free baseline polls
  `GET /v1/live-data/ltp` (≤50 symbols/call → our 23 keys in 2 calls @4 Hz).
  Poll mode carries LTP only (no greeks/depth) → strike pick uses the ATM
  fallback (§6-F5); exits stay tick-driven on those LTPs.
- **Canonical key unification (Phase 2):** feed keys are `{EXCHANGE}_{SEG}|{token}`
  on BOTH brokers (`NSE_FO|11`, `NSE_INDEX|NIFTY`) → the same contract has the
  same key shape everywhere; Groww order bodies derive `exchange`/`segment`
  from it (`NSE_FO` → `NSE` + `FNO`).

### 4.4 Broker policy
- **One active broker at a time** (selected in Settings/controls). Switching requires a flat position (enforced by Risk).
- Adapter failures never touch strategy code: `OrderAgent` sees only canonical types; a failed adapter → `risk.veto(BROKER_DOWN)` for new entries, Watchdog handles reconnect.
- Upstox built first (Phase 1), Groww second (Phase 2) — see §13.

---

## 5. Feature specification F1–F10 (requirement → design → acceptance)

### F1 — Signals & live auto trading
- Modes: **AUTO** (risk-approved signals fire immediately) / **MANUAL** (signal waits for EXECUTE click).
- Real MARKET orders only; entry price = option LTP at dispatch (no limit chasing in v1).
- **Acceptance:** in AUTO, a valid closed candle produces ≤1 order per side with no user action; in MANUAL, no order without click; both journalled with mode stamp.

### F2 — Options buying only (`SideGuard`, inside `OrderAgent`, before adapter)
Hard rejects (each journalled with reason code):
`SG_SELL_WITHOUT_LONG` (SELL with no matching open long) · `SG_SELL_EXCEEDS_QTY` (sell qty > open qty) · `SG_SELL_TO_OPEN` · `SG_NON_OPTION` (stock/future) · `SG_MULTI_LEG`.
Because `OrderRequest.intent ∈ {BUY_TO_OPEN, SELL_TO_CLOSE}` (§4.1), sell-to-open is *unrepresentable*; SideGuard is the belt-and-braces runtime check + audit source.
- **Acceptance:** unit suite fires 50 adversarial order shapes at SideGuard; 100% reject of non-close sells; position panel shows `SideGuard: SELL allowed = close-only ✓`.

### F3 — Settings → broker connections (edit / save / connect / disconnect)
- Per-broker card: API key, API secret (masked, DPAPI at rest), redirect URI (Upstox), PIN/TOTP entry method, token status LED, last error.
- Buttons: **EDIT / SAVE / CONNECT / DISCONNECT**. Connect runs login → master load → feed auth → subscribe; every step journalled; failure shows broker's error text verbatim.
- Active-broker radio (one at a time), risk/session settings, contract table, all in Settings tab (§8).
- **Acceptance:** save→restart reloads creds without re-typing; disconnect cancels subs and blocks new entries (`risk.veto(BROKER_DOWN)`).

### F4 — What is traded; header strip
- Index selector (5 indices), expiry auto from master, lot multiplier 1–10 (qty preview), mode toggle, KILL SWITCH — controls row is **global** (kill switch reachable from every tab).
- Header strip always shows NIFTY / BANKNIFTY / SENSEX LTP + direction arrow (+ ticks/s, feed latency), regardless of selected index.
- **Acceptance:** selecting MIDCPNIFTY keeps the 3-index header live; header keys subscribed even when not traded.

### F5 — Strike selection: 5 ITM + ATM + 5 OTM (11 strikes, never full chain)
- `ATM = round(spot / step) * step`; window = `ATM ± 5·step` → exactly 11 strikes; subscribe CE+PE for each (22 option feeds + spot feeds).
- Moneyness labels are **per side**: for CE, ITM = strikes below spot; for PE the mirror. Same 11 rows, labels flip.
- **Re-center rule:** when spot drifts **≥ 2 steps** from window center → rebuild window (new subs, drop old), journalled `window.rebuilt`.
- **Strike pick (not always ATM):** from the 11, choose the strike whose feed greeks give |delta| ∈ [0.45, 0.60] for the signal side (CE: +δ, PE: −δ); tie-break nearer ATM; fallback = ATM when greeks missing. Liquidity guard: bid–ask spread ≤ 2 ticks and option 1-min volume > 0, else step one strike toward ATM.
- OI within the window (from `full` mode feed) labels CALL WALL / PUT WALL rows — **display only**, no extra scanning.
- **Acceptance:** subscription count == 22 options + ≤4 spots at all times; a 2-step drift rebuilds within 1 s; pick is journalled with delta + spread.

### F6 — One signal per side, no candle spam (state machines)
Evaluated **only on closed 1-min candles** of the selected index spot — never per tick, never mid-bar. CE and PE machines are independent, but v1 allows **max 1 open position** (either side), so `HELD` suppresses both.

```
            setup BREAK on a closed candle
   DISARMED ────────────────────────────────► ARMED
      ▲                                        │ setup VALID on closed candle
      │ break / TTL expiry / position flat     ▼
      └──────────── SIGNALED ◄──────────────  ARMED
                      │   │
        execute (auto │   │ break or TTL
        or EXECUTE)   ▼   │
                    HELD ─┘(flat → DISARMED)
```
- `SIGNALED` persists (no repeats) until: executed → `HELD`; or setup **breaks** on a closed candle → `DISARMED`; or TTL *(default 3 candles, 0 = until break)* → `DISARMED`.
- **Re-arm requires the break:** from `DISARMED`, one closed candle with the setup invalid, then a closed candle with it valid again → new `signal.new`. Identical rule in MANUAL: if the user never presses EXECUTE, the signal does **not** repeat next minute.
- `HELD` (position open, either side): no new signals on either side; on flat → `DISARMED` (break already implied by time in market? **No** — flat goes to DISARMED and still needs a break candle to re-arm).
- **Acceptance:** replay a 60-candle trending session → exactly 1 signal per side per setup episode; unit test asserts zero repeat signals while setup stays valid; MANUAL no-click run produces 1 signal total per episode.

### F7 — Analysis → indicators → decision → signal → order (text Indicator Panel)
Per closed candle (selected index spot): session **VWAP**, **EMA9/EMA21**, **RSI14**, **ATR14**, **volume vs 20-candle avg**, plus explicit per-side setup validity text (`VALID — close>VWAP · EMA9>EMA21 · RSI≥55 · vol≥1.5×` / `not valid`).
Entry setup defaults *(all configurable)*:
| Side | Conditions on the CLOSED 1-min candle |
|---|---|
| CE long | close > VWAP · EMA9 > EMA21 · RSI14 ∈ [55, 78] · candle volume ≥ 1.5 × SMA20(vol) · ATR14 ≥ index min *(default: NIFTY 10 / BANKNIFTY 25 / SENSEX 30 / FINNIFTY 12 / MIDCPNIFTY 15 pts)* |
| PE long | close < VWAP · EMA9 < EMA21 · RSI14 ∈ [22, 45] · candle volume ≥ 1.5 × SMA20(vol) · ATR14 ≥ index min |
| Break (CE) | closed candle with close < VWAP **or** EMA9 ≤ EMA21 (mirror for PE) |
Indicators are incremental (O(1) per candle); RSI uses Wilder smoothing; VWAP resets at session open (09:15 IST).
- **Acceptance:** indicator values on replayed candles match a reference pandas implementation to 1e-9; panel updates only on candle close (except LTP-driven rows).

### F8 — Risk & safety guardrails (`RiskAgent` vetoes, all journalled)
| Guard | Default |
|---|---|
| Entry window | 09:17–15:15 IST |
| Hard square-off | 15:20 IST (market sell residual, cancel pending) |
| Max trades/day | 3 |
| Max daily loss | ₹2,500 |
| SL-streak stop | ≥2 consecutive SL exits → no new entries (auto-reset next session) |
| Feed stale | no tick > 5 s → no new entries; > 30 s → Watchdog reconnect |
| Position open | max 1 → new entries vetoed |
| Kill switch | cancel pending + square off open + halt Signal/Order agents (UI + hotkey) |
| Orphan position | after crash/restart, found position not in journal → square off at market *(default; alert-only mode configurable)* |
Veto reason codes: `V_WINDOW`, `V_MAXTRADES`, `V_DAILYLOSS`, `V_SLSTREAK`, `V_STALE`, `V_HELD`, `V_BROKER`, `V_KILL`, `V_EXPIRY_ROLL`.

### F9 — Journal / audit trail
SQLite WAL, single writer (`JournalAgent`), tables: `sessions, events, signals, orders, fills, positions, risk_veto, agent_health` (DDL §10). Journal tab = filterable viewer + CSV export. Every signal/order/veto/state-change is durably written **before** the action is acknowledged on the bus (write-ahead for orders).

### F10 — Agents tab / supervisor
13-row health table: state, uptime, restarts, inbox depth, p99 processing lag, last heartbeat; bus topic rates; Supervisor restart-with-backoff on crash; crash-injection test hook (§12).

---

## 6. Strategy exits — T1 / T2 / T3 / SL (`PositionExitAgent`)

Premium-point targets *(defaults, configurable 1–30)*: **T1 +5 · T2 +12 · T3 +20 · SL −8**, measured from average entry price, evaluated on **option LTP ticks** (exits are tick-driven for speed; entries are candle-driven).

**Whole-lot scaling schedule** (exchange trades whole lots only):
| Lots N | T1 | T2 | T3 | SL / time-stop / square-off |
|---|---|---|---|---|
| N ≥ 2 | sell floor(N/2) lots | sell floor(remaining/2) lots | sell all remaining | sell all remaining |
| N = 1 | no partial — trail SL → entry (breakeven) | trail SL → entry + T1 pts | sell all at market | sell all |

- After any partial, SL for the residual follows the trail ladder above; targets never widen risk.
- **Time-stop:** position age > 15 candles *(default)* → exit residual at market.
- Exits are always `SELL_TO_CLOSE` of residual qty; SideGuard re-checks on every exit order.
- **Acceptance:** unit tests over synthetic fill sequences (recorded-shape, not live signals) assert: qty conservation, no negative position ever, SL after T1 never below BE for N=1, 15:20 always flattens.

---

## 7. UI design (see `mock.html` for the pixel layout)

Global chrome (all tabs): **header LTP strip** + **controls row** (index, expiry, lots, mode, KILL SWITCH) + **status bar**.
Tabs: **TRADE · SETTINGS · JOURNAL · AGENTS**. Dark GitHub-style theme, monospace numerals, **zero charts**.

TRADE tab, top→bottom (matches mock):
1. Indicator Panel — last *closed* 1-min candle O/H/L/C + VWAP, EMA9/21, RSI, ATR, vol×, CE/PE setup validity text. *(F7)*
2. Signal Panel — CE row + PE row with state dots (`DISARMED/ARMED/SIGNALED/HELD`), EXECUTE button (MANUAL) or auto-fire chip (AUTO), re-arm rule text, agents 13/13, daily counters (trades x/3, SL-streak, P&L). *(F1, F6)*
3. Strike Window — exactly 11 rows (5 ITM + ATM + 5 OTM), CE/PE LTPs, Γ, wall tags, `◀ SELECTED` highlight. *(F5)*
4. Open Position — instrument, qty, avg/LTP, unrealized P&L (pts + ₹), target chips T1✓/T2/T3/SL, trail note, SideGuard status, **EXIT (SELL TO CLOSE)** button. *(F2, §6)*
5. Trade/Signal Log — timestamped bus choreography trace (`candle.closed → indicators.ready → signal.new → strike.selected → order.approved → order.req → order.fill (71 ms)`). *(F9)*

SETTINGS tab: broker cards (edit/save/connect/disconnect, LEDs, errors), active-broker radio, risk/session defaults table, contract table (step/lot/exchange), data-dir + replay toggle. *(F3, F4, F8)*
JOURNAL tab: filterable audit table + CSV export. *(F9)*
AGENTS tab: 13-agent health table + bus rates + supervisor controls. *(F10)*

**UI transport:** JS→Py commands via pywebview `js_api`: `execute_signal(side)`, `set_mode`, `set_index`, `set_lots`, `kill_switch`, `exit_position`, `broker_connect/disconnect/save`, `journal_filter`, `export_csv`. Py→UI: one `orjson` snapshot at 10–20 Hz (diffed client-side). No polling loops in JS.

**Differences vs the attached reference mock (`ui_mockup.html`)** — deliberate:
1. **Gamma / hero-zero panel removed** — it needs OI/premium scans outside the 11-strike window, which §0.3 forbids. (Open question Q2.)
2. **Tabs added** (Settings/Journal/Agents) — required by F3/F9/F10; reference mock was single-page.
3. Controls/header/status bar made **global** so KILL SWITCH is reachable from every tab.
4. Mode default shown as MANUAL with live EXECUTE (better button-state demo); AUTO shows an auto-fire chip instead.
5. Feature badges `[F1]…[F10]` on each panel tie layout → spec → tests.

---

## 8. Settings schema (`settings.json`, secrets excluded)

```jsonc
{
  "broker_active": "upstox",
  "index": "BANKNIFTY",
  "lot_multiplier": 1,                 // 1..10
  "mode": "MANUAL",                    // AUTO | MANUAL
  "targets": { "t1": 5, "t2": 12, "t3": 20, "sl": 8 },        // premium points
  "trail_n1": ["BE", "T1"],            // N=1 ladder after T1, T2
  "time_stop_candles": 15,
  "signal_ttl_candles": 3,             // 0 = hold until break
  "entry_window": ["09:17", "15:15"],  // IST
  "square_off": "15:20",
  "max_trades_per_day": 3,
  "max_daily_loss_inr": 2500,
  "sl_streak_stop": 2,
  "stale_feed_s": 5,
  "reconnect_stale_s": 30,
  "orphan_policy": "square_off",       // square_off | alert_only
  "setup": {
    "rsi_ce": [55, 78], "rsi_pe": [22, 45],
    "vol_mult": 1.5,
    "atr_min": { "NIFTY": 10, "BANKNIFTY": 25, "SENSEX": 30, "FINNIFTY": 12, "MIDCPNIFTY": 15 },
    "delta_band": [0.45, 0.60],
    "max_spread_ticks": 2
  },
  "recenter_steps": 2,
  "replay": false
}
```

---

## 9. Journal DDL (SQLite, WAL)

```sql
CREATE TABLE sessions(id INTEGER PRIMARY KEY, ts_start TEXT, ts_end TEXT, broker TEXT, index_symbol TEXT, mode TEXT);
CREATE TABLE events(seq INTEGER PRIMARY KEY AUTOINCREMENT, ts_utc TEXT, mono_ns INTEGER,
                    topic TEXT, agent TEXT, level TEXT, payload TEXT);           -- orjson blob
CREATE TABLE signals(id INTEGER PRIMARY KEY AUTOINCREMENT, session_id INT, ts TEXT, side TEXT,
                     state_from TEXT, state_to TEXT, candle_ts TEXT, reason TEXT,
                     indicators TEXT, tick_to_signal_ms REAL);
CREATE TABLE orders(id INTEGER PRIMARY KEY AUTOINCREMENT, session_id INT, client_id TEXT UNIQUE,
                    ts_req TEXT, ts_ack TEXT, ts_fill TEXT, broker TEXT, instrument TEXT,
                    intent TEXT, qty INT, price REAL, status TEXT, latency_ms REAL, error TEXT);
CREATE TABLE positions(id INTEGER PRIMARY KEY AUTOINCREMENT, session_id INT, instrument TEXT,
                       qty_in INT, qty_out INT, avg_in REAL, pnl_inr REAL, pnl_pts REAL,
                       exit_reason TEXT, opened_ts TEXT, closed_ts TEXT);
CREATE TABLE risk_veto(id INTEGER PRIMARY KEY AUTOINCREMENT, session_id INT, ts TEXT,
                       reason_code TEXT, context TEXT);
CREATE TABLE agent_health(ts TEXT, agent TEXT, state TEXT, restarts INT, inbox INT, p99_ms REAL);
CREATE INDEX ix_events_topic ON events(topic, ts_utc);
CREATE INDEX ix_orders_session ON orders(session_id, ts_req);
```

---

## 10. Repository layout (to be created at build start)

```
Scapler/
├─ plan.md                  ← this document
├─ mock.html                ← layout mock (confirmation artifact)
├─ ui_mockup.html/.png      ← reference mock (kept)
├─ scapler/
│  ├─ core/        bus.py · agent.py · messages.py · clock.py · config.py
│  ├─ agents/      market_data.py · candle.py · indicator.py · signal.py · strike.py
│  │               risk.py · order.py · position_exit.py · connection.py · watchdog.py
│  │               journal.py · ui.py · supervisor.py
│  ├─ brokers/     base.py · normalize.py · upstox/ (rest.py, feed_v3.py, proto/) · groww/ (rest.py, feed.py)
│  ├─ strategy/    setup.py · exits.py · strikes.py
│  ├─ ui/          host.py (pywebview) · frontend/ (index.html, app.js, style.css)
│  ├─ data/        journal.db · masters/ · replays/        (git-ignored)
│  └─ main.py
├─ tests/          unit/ (sideguard, statemachine, strikes, exits, indicators)
│                  integration/ (bus, replay harness) · bench/ (latency p99) · crash/
├─ packaging/      scapler.spec · installer.iss
└─ pyproject.toml
```

---

## 11. Testing & validation

| Level | What | Gate |
|---|---|---|
| Unit | SideGuard adversarial suite; state-machine spam suite (60-candle trend → 1 signal/episode); strike window math (drift, roll, per-side moneyness); exit scheduler qty conservation; indicators vs pandas reference | 100% pass in CI |
| Integration | EventBus choreography on **recorded real ticks** (REPLAY banner): candle close → signal → (stub broker) fill → exits | scenario suite green |
| Broker sandbox | Upstox/Groww connect, master parse, subscribe 22 keys, place→cancel 1-lot order in presence of real session (go-live checklist item) | manual sign-off |
| Crash | Supervisor kill-injection: agents restart, journal intact, orphan policy fires | no lost orders, no double entries |
| Latency bench | recorded tick burst → assert tick→signal p99 < 50 ms, dispatch < 10 ms | bench report in Journal tab |
| Live shadow | Phase 7: full stack live, orders stubbed at adapter edge (signals + risk decisions journalled, compare vs manual expectation) | ≥3 clean sessions |

---

## 12. Roadmap (~3 weeks, phases independently testable)

**Build log**
- 2026-09-22 — Phase 0 ✅ : EventBus/Agent/messages/config + tests + bench PASS
  (565k msg/s, p99 0.17 ms). Strategy core landed early and fully tested:
  `strategy/setup.py`, `signal_fsm.py` (F6), `strikes.py` (F5), `exits.py` (§6),
  `sideguard.py` (F2). 61 unit tests green.
- 2026-09-22 — Phase 1 ✅ (code-complete, unit-verified): `brokers/base.py` ABC;
  Upstox adapter — REST v2 (OAuth token, orders, positions), instrument-master
  CSV parser (nearest live expiry, lots, OPTIDX-only), **dependency-free
  protobuf wire decoder** for Streamer v3 against the officially vendored
  `MarketDataFeedV3.proto`, feed handle (authorize→wss→sub→Ticks, drop-oldest
  queue), tick recorder/replayer. 89 tests green. Live 1-lot place→cancel
  remains a Phase-7 go-live checklist item (needs real credentials).
- 2026-09-22 — Phase 2 ✅ : Groww adapter (REST v1: token/order/cancel/positions/
  ltp-batch/quote, instrument.csv parser, poll feed handle) + **cross-broker
  parity suite** (same market on both wires → identical canonical expiries,
  lots, strikes, order semantics, position qty/avg, tick prices; unified
  `{EX}_{SEG}|token` feed keys). 111 tests green.
- 2026-09-22 — Phase 3 ✅ : `agents/market_data.py` (feed handle owner, ticks/s,
  staleness), `agents/candle.py` (closed-1m-bars only: next-tick close + wall-
  clock quiet-flush, late-tick drop, cumulative-volume deltas with tick-activity
  proxy for volume-less index feeds), `agents/indicator.py` +
  `strategy/indicators.py` (incremental VWAP/EMA9/21/RSI14-Wilder/ATR14/vol×,
  neutral warm-up constants that gates reject). Chain integration test proves
  ticks→candles→indicators→setup→FSM emits exactly ONE signal per trending
  episode; indicators match pandas (EMA/VWAP) + textbook loop to 1e-9.
  123 tests green. Live Groww validation tool added
  (`tools/run_groww_validation.sh`, read-only) — sandbox egress cannot reach
  groww.in, so it runs on the operator machine and emits fixtures that
  `test_live_fixtures.py` picks up automatically.
- 2026-09-22 — Phase 4 ✅ : `core/ids.py` (Groww-compatible client ids
  `SCP-{yyMMdd}-{seq:04d}`, ≤20 chars), `agents/signal.py` (FSM on the bus:
  AUTO auto-executes, MANUAL waits for `ui.execute`, re-arm only after setup
  break; POSITION_UPDATE keeps HELD in sync), `agents/strike.py` (owns the
  5-ITM+ATM+5-OTM window, builds after first spot tick, re-centers on ±2-step
  drift with `window.rebuilt`, delta-band pick |δ|∈[0.45,0.60] → ATM fallback,
  qty = master lot × clamp(multiplier,1,10)), `agents/risk.py` (every veto
  code V_KILL/V_HELD/V_WINDOW/V_MAXTRADES/V_DAILYLOSS/V_SLSTREAK/V_STALE;
  SELL_TO_CLOSE never vetoed), `agents/order.py` (SideGuard F2 gate,
  idempotent by client_id, MARKET fills via broker adapter),
  `agents/position_exit.py` (per-fill tracker: SL → T1 (stop→BE) → T2 → T3,
  time-stop on candle close, KILL force-close, realized P&L + exit_reason on
  `position.update`). messages: SIGNAL_EXECUTE/UI_EXECUTE topics,
  OrderFill.lot_size, PositionUpdate struct; Settings gains exchange step
  table. `tests/_master.py` + `tests/_broker.py` fixtures; e2e loop test
  drives spot ticks → one CE signal → BUY 30 → exit ladder sells → HELD
  released → no second entry. 146 tests green (+2 skipped live-fixture),
  bus bench still PASS (478k msg/s, p99 0.22 ms).
- 2026-09-22 — Phase 5 ✅ : UI shell per approved `mock.html` (text-only,
  dark GitHub theme, zero charts, monospace tabular numerals). New agents:
  `agents/ui.py` (#12 — subscribes to every UI-relevant topic, derives
  counters/positions from bus events, coalesced snapshots at 10 Hz on
  `ui.snapshot`, JS commands → bus: execute/kill/exit_position/set_mode/
  set_lots/save_settings/broker_*), `agents/supervisor.py` (#13 — 1 Hz
  `agent.health` fan-out for the Agents tab; full crash-injection &
  watchdog orchestration stay Phase 6). Frontend `scapler/ui/web/`
  (index.html/app.css/app.js): global header LTP strip + controls row +
  KILL SWITCH modal reachable from every tab; TRADE (Indicator/Signal/
  Strike-Window/Open-Position/Log panels), SETTINGS (broker cards, risk
  table, contracts), JOURNAL (filter + CSV export, in-memory until Phase 6
  SQLite), AGENTS (health table + bus rates). Transports: pywebview/WebView2
  js_api + evaluate_js (desktop, no HTTP) and an aiohttp dev server
  (WS snapshots + POST /api/cmd) for operator-browser preview.
  `scapler/ui/runtime.py` wires the 10-agent stack; `--demo` runs a clearly
  labelled synthetic 60-min day (chained future minute grids, ATM±2 quotes,
  StubBroker) — demo banner + stub fills, never live. Backend support:
  `window.rebuilt` now announced on first build with strikes/map/step and
  consumed by MarketData (resub footprint); `position.update` carries
  t1_hit/t2_hit/sl_price (trail changes announced between fills);
  ExitTrigger(MANUAL) from the UI closes the tracker (reason MANUAL).
  Validated live over the dev server: 8 trades/60 s, delta-band picks on
  both sides, T1/T2 trail flags + BE ratchet visible, T3/SL/TIME_STOP
  exits, EXIT button → SELL_TO_CLOSE fill → flat, KILL → force square-off
  + EXECUTE disabled. 150 tests green (+2 skipped), bench PASS.
- 2026-09-23 — Phase 6 ✅ : journal/watchdog/connection agents → the full
  13-agent roster is live. `agents/journal.py` (SQLite WAL, synchronous=FULL
  commits on the order path before the next message is consumed; tables
  sessions/events/signals/orders/fills/positions/risk_veto/agent_health;
  EXPORT CSV streams the DB). `agents/watchdog.py` (stale-feed →
  `feed.reconnect` with reconnect_stale_s cooldown, square-off at 15:20 IST
  via kill.switch(source=watchdog) → PositionExit books SQUARE_OFF (not
  KILL), orphan-policy hook, 1 Hz `watchdog.status` for the status bar).
  `agents/connection.py` (both brokers, one active: secrets →
  SecretStore → login → master via memory→disk→network cache →
  `connection.status`; daily token refresh 09:10–09:16). `core/
  secrets_store.py` (Windows DPAPI CryptProtectData; 0600 plaintext dev
  fallback with warning — never committed, git-ignored). `brokers/
  master_cache.py` (same-day JSON cache, corrupt-file refetch, 3-day
  pruning). **User directives:** index priority NIFTY (default) → SENSEX →
  BANKNIFTY → FINNIFTY → MIDCPNIFTY everywhere (Settings order drives the
  dropdown); index switch is INSTANT — no restart prompt: candle/signal/
  strike/UI hot-rebind + feed reconnect against the memory-cached master,
  choice persisted to settings.json; refused only while a position is open
  (one index at a time). Demo feed now streams all five indices (spots +
  ATM±2 quotes, phase-staggered episodes) so every dropdown entry has live
  data. Packaging: `scapler/__main__.py` entry (`scapler` console script),
  PyInstaller `packaging/scapler.spec` (onedir, windowed, web assets
  bundled, tkinter/pandas excluded) + `tools/build_windows.ps1`.
  Verified live over the dev server: NIFTY default (lot 65), instant switch
  SENSEX (lot 20, window 79500–80500 BSE) → BANKNIFTY (lot 30) → back to
  NIFTY, 2 trades + ladder exits on NIFTY, journal WAL 676 events /
  35 orders / 7 fills, CSV export, 13/13 agents healthy. 174 tests green
  (+2 skipped), bench PASS.
- 2026-09-23 — Phase 7 ✅ : SHADOW MODE + go-live tooling — the last step
  before real money. `Settings.shadow` (default **ON**): the full stack runs
  on the real feed, but OrderAgent stubs the adapter edge — fills are
  computed at the REAL prevailing quotes (BUY at ask, SELL at bid, LTP
  fallback when the book is empty), order ids `SHADOW-{client_id}`, and
  **no broker call is made**; SideGuard, risk counters, journal and UI all
  run for real. Shadow flips live from the Settings tab (persisted to
  settings.json; flipping OFF warns "LIVE ORDERS will be sent"). UI: blue
  "SHADOW MODE — no real orders" banner + status-bar tag; the KILL button
  becomes **RE-ARM** once halted (refused while a position is open; clears
  risk/watchdog/supervisor/UI kill flags). Daily rollover: WatchdogAgent
  watches the IST date (injectable `date_fn`) and publishes
  `session.new_day` → RiskAgent resets trades/P&L/SL-streak/kill, Watchdog
  clears squared_off, JournalAgent closes the session and opens a new one
  (sessions labelled `AUTO/SHADOW` in shadow), UIAgent resets its counters.
  Tools: `tools/shadow_report.py` (per-session cleanliness: signals,
  fills, SG_* rejects, veto histogram, P&L by exit reason, fill-latency
  p50/p99; "go-live ready" ⇔ ≥3 clean non-demo sessions) and
  `tools/go_live_check.py` (§13 checklist: secrets present, master cached
  today + lot/step sanity vs the plan table — master wins, journal
  WAL/sessions/export, ≥3 clean shadow sessions, breakers configured,
  square-off 15:20, plus MANUAL sign-off items and a win32 powercfg sleep
  check; verdict GO / NO-GO / manual-signoff). Verified live over the dev
  server: shadow default ON in the snapshot, toggle OFF→ON round-trip with
  notes, kill → RE-ARM clears the halt, session 3 journalled as
  MANUAL/SHADOW, both tools run against the live journal (correct NO-GO in
  the sandbox: no secrets, 0/3 clean sessions). 190 tests green
  (+2 skipped), bench PASS.



| Phase | Scope | Exit criteria | Days |
|---|---|---|---|
| 0 | Repo skeleton, EventBus + Agent base, msgspec catalog, CI, logging off hot path | bus bench: 100k msgs/s, p99 < 1 ms | 1–2 |
| 1 | Upstox adapter: OAuth, master, Streamer v3 feed, order place/cancel | live subs 22 keys, ticks/s visible, 1-lot place→cancel | 3–4 |
| 2 | Groww adapter (same ABC) + normalization parity tests | parity suite vs Upstox shapes | 2–3 |
| 3 | Candles + indicators **validated against broker feed values** + signal state machines + strike window | replay spam-tests green; indicator parity 1e-9 | 2–3 |
| 4 | OrderAgent + SideGuard + Risk vetoes + PositionExit ladder | adversarial suites green; qty-conservation proof | 3 |
| 5 | UI (pywebview + frontend), text panels, **no charts**, tabs, kill switch | snapshot 10–20 Hz, RSS < 120 MB | 3–4 |
| 6 | Journal viewer, Agents tab, Watchdog/reconnect, orphan policy, packaging (PyInstaller + Inno) | installer on clean Win11 VM | 2–3 |
| 7 | Shadow-mode live run → go-live checklist → real 1-lot run | ≥3 clean shadow sessions, sign-off | 3–5 |

---

## 13. Go-live checklist (abridged)
Broker creds in DPAPI · master lots == table or acknowledged · feed latency < 50 ms · shadow sessions clean · breakers set (max trades/loss) · square-off time correct IST · kill switch drilled (manual + hotkey) · journal export verified · UPS/laptop power plan · Windows sleep disabled during session.

---

## 14. Risks & mitigations
| Risk | Mitigation |
|---|---|
| WS drop mid-position | Watchdog reconnect + orphan square-off policy + tick-driven exits re-armed from journal on restart |
| Token expiry mid-session | ConnectionAgent pre-market refresh + auto re-login window 09:10–09:16 |
| Lot-size/step change by exchange | master-derived values, table only sanity-checks |
| Slippage on MARKET exits | tick-size-aware expectations; SL evaluated on LTP with 1-tick buffer *(default, configurable)* |
| UI freeze | UI never on hot path; coalesced snapshots only |
| Dual-broker confusion | one active broker enforced; switch requires flat |

---

## 15. Open questions — **confirm before build starts**

| Q | Question | Recommendation |
|---|---|---|
| Q1 | Is `mock.html` layout approved as the build target (tabs + 5 TRADE panels + global chrome)? | approve |
| Q2 | Gamma/hero-zero panel from the reference mock: **drop in v1** (needs scans beyond the 11-strike window → conflicts NON-GOALS), keep as post-v1 optional tab, or force into v1? | drop in v1 |
| Q3 | 1-lot exit rule: trail-only ladder (T1→BE, T2→+T1, T3→flat) since a single lot can't be partially sold? | trail-only |
| Q4 | Build order: Upstox adapter first, Groww second? | yes |
| Q5 | Signal TTL default 3 closed candles (then requires break to re-arm), or hold SIGNALED until break only (TTL=0)? | 3 candles |
| Q6 | Max 1 open position across both sides in v1 (HELD suppresses CE+PE)? | yes |

---

*End of plan. Build begins only after Q1–Q6 are confirmed against `mock.html`.*
