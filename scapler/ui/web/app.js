/* SCAPLER UI — vanilla JS, text-only, zero charts.
 *
 * Transport bridge:
 *   production : pywebview js_api — commands via pywebview.api.cmd(name, json),
 *                snapshots pushed by Python through window.__snap(obj)
 *   dev/preview: WebSocket — snapshots arrive {t:"snap"}, commands sent
 *                {t:"cmd", name, args}; falls back to POST /api/cmd
 *
 * Rendering: one snapshot object → DOM. Sections cache their last JSON so
 * tables are only rebuilt when their data actually changed (10 Hz friendly).
 */
"use strict";

const $ = (id) => document.getElementById(id);
const cache = {};
function changed(key, val) {
  const s = JSON.stringify(val);
  if (cache[key] === s) return false;
  cache[key] = s;
  return true;
}
function fmt(x, nd = 2) {
  if (x === null || x === undefined) return "—";
  return Number(x).toLocaleString("en-IN", {
    minimumFractionDigits: nd, maximumFractionDigits: nd });
}
function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}
let toastTimer = null;
function toast(msg, isErr = false) {
  const t = $("toast");
  t.textContent = msg;
  t.className = isErr ? "err" : "";
  t.style.display = "block";
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { t.style.display = "none"; }, 4200);
}

/* ───────────────────────── transport ───────────────────────── */
let ws = null;
const isWebview = () => !!(window.pywebview && window.pywebview.api);

function cmd(name, args = {}) {
  if (isWebview()) {
    window.pywebview.api.cmd(name, JSON.stringify(args))
      .then((r) => handleResult(name, JSON.parse(r)))
      .catch((e) => toast(`${name}: ${e}`, true));
    return;
  }
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ t: "cmd", name, args }));
    return;
  }
  fetch("api/cmd", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name, args }),
  }).then((r) => r.json()).then((r) => handleResult(name, r))
    .catch((e) => toast(`${name}: ${e}`, true));
}

function handleResult(name, r) {
  if (r && r.ok === false) toast(`${name}: ${r.error || "rejected"}`, true);
  else if (r && r.note) toast(r.note);
  else if (r && r.restart_required && r.restart_required.length)
    toast(`saved; restart required for: ${r.restart_required.join(", ")}`);
}

function connectWS() {
  const proto = location.protocol === "https:" ? "wss://" : "ws://";
  ws = new WebSocket(proto + location.host + "/ws");
  ws.onmessage = (m) => {
    try {
      const d = JSON.parse(m.data);
      if (d.t === "snap") window.__snap(d.snap);
      else if (d.t === "cmd_result") handleResult("cmd", d);
    } catch (e) { /* ignore malformed */ }
  };
  ws.onclose = () => setTimeout(connectWS, 1500);
  ws.onerror = () => { try { ws.close(); } catch (e) {} };
}
if (!isWebview()) {
  if (document.readyState === "complete") connectWS();
  else window.addEventListener("load", connectWS);
}

/* ───────────────────────── rendering ───────────────────────── */
let lastSnap = null;

window.__snap = function (snap) {
  lastSnap = snap;
  try { render(snap); } catch (e) { console.error("render", e); }
};

function render(s) {
  /* banners */
  $("banner-demo").style.display = s.demo ? "" : "none";
  $("banner-shadow").style.display = (!s.demo && s.shadow) ? "" : "none";
  $("banner-replay").style.display =
    (!s.demo && s.settings && s.settings.replay) ? "" : "none";

  /* header */
  const hl = s.header.ltps || {};
  for (const name of ["NIFTY", "BANKNIFTY", "SENSEX"]) {
    const el = $("ltp-" + name);
    const v = hl[name];
    if (!el) continue;
    if (v && v.ltp !== null && v.ltp !== undefined) {
      el.textContent = fmt(v.ltp);
      el.className = "mono " + (v.dir || "");
    } else el.textContent = "—";
  }
  $("brokername").textContent = s.header.broker;
  $("brokerled").className = "led " + (s.demo ? "demo" :
    s.header.conn_state === "CONNECTED" ? "on" :
    s.header.conn_state === "ERROR" ? "err" : "off");
  $("tickrate").textContent = s.header.ticks_per_s ?? 0;
  $("clock").textContent = s.ts;

  /* controls — dropdown rebuilt from backend order (NIFTY, SENSEX,
     BANKNIFTY, …) so the priority list lives in Settings, not in markup */
  const ctl = s.controls;
  const sel = $("idxsel");
  const want = (ctl.indices || []).join(",");
  if (cache["idxopts"] !== want) {
    cache["idxopts"] = want;
    sel.innerHTML = (ctl.indices || []).map((i) =>
      `<option${i === ctl.index ? " selected" : ""}>${i}</option>`).join("");
  }
  sel.value = ctl.index;
  $("expiry").textContent = ctl.expiry ?
    `${ctl.expiry} · auto(master)` : "—";
  $("lots").textContent = ctl.lots;
  $("qty").textContent = ctl.qty || "—";
  const auto = s.mode === "AUTO";
  $("m-auto").className = "radio" + (auto ? " on" : "");
  $("m-auto").textContent = (auto ? "● " : "○ ") + "AUTO";
  $("m-manual").className = "radio" + (!auto ? " on" : "");
  $("m-manual").textContent = (!auto ? "● " : "○ ") + "MANUAL";
  $("kill").className = s.killed ? "armed rearm" : "";
  $("kill").textContent = s.killed ? "● RE-ARM (halted)" : "■ KILL SWITCH";

  renderIndicators(s);
  renderSignals(s);
  renderWindow(s);
  renderPosition(s);
  renderLog(s);
  renderSettings(s);
  renderJournal(s);
  renderAgents(s);
  renderStatus(s);
}

function renderIndicators(s) {
  const i = s.indicators;
  if (!i) {
    $("ind-candle").textContent = "waiting for first closed candle…";
    return;
  }
  $("ind-candle").innerHTML =
    `${i.time} &nbsp;O ${fmt(i.o, 1)} · H ${fmt(i.h, 1)} · L ${fmt(i.l, 1)} · C ${fmt(i.c, 1)}`;
  $("ind-vwap").textContent = fmt(i.vwap);
  const ema = $("ind-ema");
  ema.textContent = `${fmt(i.ema9)} / ${fmt(i.ema21)}`;
  ema.className = "mono " + (i.ema9 > i.ema21 ? "up" : "dn");
  $("ind-rsi").textContent = i.rsi ?? "—";
  $("ind-atr").textContent = i.atr ?? "—";
  const vol = $("ind-vol");
  vol.textContent = i.vol_ratio !== null ? `${i.vol_ratio}×` : "—";
  vol.className = "mono " + (i.vol_ratio >= 1.5 ? "up" : "");
  const ce = $("ind-setup-ce"), pe = $("ind-setup-pe");
  ce.textContent = i.setup_ce; ce.className = i.setup_ce.startsWith("VALID") ? "up" : "dim";
  pe.textContent = i.setup_pe; pe.className = i.setup_pe.startsWith("VALID") ? "up" : "dim";
}

function renderSignals(s) {
  const sig = s.signals;
  $("sig-ttl").textContent =
    `${sig.ttl} closed candles, then needs break`;
  $("sig-agents").textContent =
    `${(s.agents || []).length} / ${(s.agents || []).length + (s.agents_planned || []).length} running ●`;
  const t = sig.today;
  $("sig-today").innerHTML =
    `trades ${t.trades}/${t.max} · SL-streak ${t.sl_streak} · P&amp;L ` +
    `<span class="${t.pnl >= 0 ? "up" : "dn"}">${t.pnl >= 0 ? "+" : ""}₹${fmt(t.pnl, 0)}</span>`;
  for (const side of ["CE", "PE"]) {
    const d = sig.sides[side], st = d.state;
    const row = $("sigrow-" + side), dot = $("sigdot-" + side);
    const state = $("sigstate-" + side), line = $("sigline-" + side);
    const btn = $("exec-" + side);
    row.className = "sig " + (st === "SIGNALED" ? "live" :
      st === "HELD" ? "held" : st === "ARMED" ? "armed" : "idle");
    dot.className = "dot " + (st === "SIGNALED" ? "on" :
      st === "HELD" ? "held" : st === "ARMED" ? "armed" : "off");
    state.textContent = `${side} · ${st}`;
    state.className = st === "SIGNALED" ? "up" : st === "HELD" ? "bl" :
      st === "ARMED" ? "gd" : "dim";
    line.textContent = d.line || d.reason || "no setup — waiting for closed candle";
    line.className = "mono " + (d.line ? "" : "dim");
    if (s.mode === "AUTO") {
      btn.className = "chipauto"; btn.textContent = "AUTO-FIRE ✓";
      btn.disabled = true; btn.style.pointerEvents = "none";
    } else {
      btn.className = "btn" + (d.can_execute ? "" : " gray");
      btn.textContent = "EXECUTE";
      btn.disabled = !d.can_execute || s.killed;
      btn.style.pointerEvents = "";
    }
  }
}

function renderWindow(s) {
  const w = s.window;
  const body = $("window-body");
  if (!w.rows || !w.rows.length) return;
  if (!changed("window", w)) return;
  let html = "";
  for (const r of w.rows) {
    const sel = w.selected && Math.abs(w.selected.strike - r.strike) < 0.01;
    const cls = sel ? "hot" : "dimrow";
    const note = sel ?
      `◀ SELECTED ${w.selected.side} δ${w.selected.delta ?? "?"}` :
      r.ce_delta !== null ? `δ${r.ce_delta > 0 ? "+" : ""}${r.ce_delta}` : "";
    html += `<tr class="${cls}"><td>${r.tag}</td><td>${fmt(r.strike, 0)}</td>` +
      `<td>${r.gamma !== null ? r.gamma.toFixed(4) : "—"}</td>` +
      `<td class="${sel ? "" : "hotcell"}">${r.ce_ltp !== null ? fmt(r.ce_ltp) : "—"}</td>` +
      `<td class="${sel ? "" : "hotcell"}">${r.pe_ltp !== null ? fmt(r.pe_ltp) : "—"}</td>` +
      `<td>${note}</td></tr>`;
  }
  body.innerHTML = html;
}

function renderPosition(s) {
  const p = s.position;
  $("pos-empty").style.display = p.open ? "none" : "";
  $("pos-body").style.display = p.open ? "" : "none";
  if (!p.open || !changed("pos", p)) {
    if (!p.open) delete cache["pos"];
    return;
  }
  $("pos-inst").textContent = p.instrument;
  $("pos-qty").textContent = `${p.qty} (${p.lot} × ${p.lots} lot${p.lots > 1 ? "s" : ""})`;
  $("pos-avg").innerHTML = `${fmt(p.avg)} / <span class="${p.ltp >= p.avg ? "up" : "dn"}">${fmt(p.ltp)}</span>`;
  const u = $("pos-upnl");
  u.innerHTML = `<span class="${p.upnl_inr >= 0 ? "up" : "dn"}">` +
    `${p.upnl_inr >= 0 ? "+" : ""}₹${fmt(p.upnl_inr)} ` +
    `(${p.upnl_pts >= 0 ? "+" : ""}${fmt(p.upnl_pts)} pts)</span>`;
  const tg = p.targets;
  $("t-t1").textContent = `T1 +${tg.t1}${p.t1 ? " ✓" : ""}`;
  $("t-t1").className = "t" + (p.t1 ? " hit" : "");
  $("t-t2").textContent = `T2 +${tg.t2}${p.t2 ? " ✓" : ""}`;
  $("t-t2").className = "t" + (p.t2 ? " hit" : "");
  $("t-t3").textContent = `T3 +${tg.t3}`;
  $("t-sl").textContent = `SL −${tg.sl}`;
  $("pos-trail").textContent = p.trail;
  $("pos-sl").textContent = p.sl_price ? fmt(p.sl_price) : "—";
  $("pos-time").textContent = p.time_stop;
}

function renderLog(s) {
  if (!changed("log", s.log)) return;
  const el = $("log");
  el.innerHTML = s.log.length ? s.log.slice().reverse().map((r) =>
    `<div><span class="dim">${r.ts}</span> <span class="${r.cls}">${esc(r.topic)} ${esc(r.detail)}</span></div>`
  ).join("") : '<div class="dim">waiting for events…</div>';
}

function renderSettings(s) {
  const st = s.settings;
  const conn = s.connection || {};
  if (!changed("settings", st) && !changed("conn", conn)) return;
  const isAct = (b) => conn.active === b || st.broker_active === b;
  $("active-upstox").textContent = isAct("upstox") ? "● active broker" : "";
  $("active-groww").textContent = isAct("groww") ? "● active broker" : "";
  for (const [b, el, led] of [["upstox", "up-status", "led-upstox"],
                              ["groww", "gr-status", "led-groww"]]) {
    const mine = conn.broker === b ? conn : null;
    const txt = mine ?
      `${mine.state}${mine.error ? " — " + mine.error : ""}` +
      (mine.master_source ? ` · master ${mine.master_source}` : "") :
      "not connected";
    $(el).textContent = txt;
    $(el).className = "mono " + (mine && mine.state === "CONNECTED" ? "up" :
      mine && mine.state === "ERROR" ? "dn" : "dim");
    $(led).className = "led " + (mine && mine.state === "CONNECTED" ? "on" :
      mine && mine.state === "ERROR" ? "err" : "off");
  }
  if (!changed("settings2", st)) return;
  $("set-maxtrades").value = st.max_trades_per_day;
  $("set-maxloss").value = st.max_daily_loss_inr;
  $("set-slstreak").value = st.sl_streak_stop;
  $("set-stale").value = st.stale_feed_s;
  $("set-reconnect").value = st.reconnect_stale_s;
  $("set-ttl").value = st.signal_ttl_candles;
  $("set-timestop").value = st.time_stop_candles;
  $("set-window").textContent = `${st.entry_window[0]}–${st.entry_window[1]}`;
  $("set-squareoff").textContent = st.square_off;
  $("set-targets").textContent =
    `+${st.targets.t1} / +${st.targets.t2} / +${st.targets.t3} / −${st.targets.sl} pts`;
  $("set-orphan").textContent = st.orphan_policy;
  $("set-replay").textContent = st.replay ? "ON (labelled)" : "OFF (live only)";
  const sh = $("shadow-toggle");
  sh.textContent = st.shadow ? "ON — flip to LIVE" : "OFF — flip to SHADOW";
  sh.className = "btn sm " + (st.shadow ? "ghost" : "red");
  const LOTS = { NIFTY: 65, BANKNIFTY: 30, SENSEX: 20, FINNIFTY: 60, MIDCPNIFTY: 120 };
  $("contracts-body").innerHTML = Object.entries(st.steps).map(([idx, step]) =>
    `<tr class="${idx === s.index ? "hot" : ""}"><td>${idx}</td><td>${step}</td>` +
    `<td>${LOTS[idx] ?? "?"}</td>` +
    `<td class="${idx === s.index ? "" : "dim"}">${idx === s.index ? "selected" : "table default · master validates"}</td></tr>`
  ).join("");
}

let journalFilter = "";
function renderJournal(s) {
  const rows = (s.journal || []).filter((r) =>
    !journalFilter || r.topic.includes(journalFilter) || r.detail.includes(journalFilter));
  if (!changed("journal", rows)) return;
  $("journal-body").innerHTML = rows.length ? rows.slice().reverse().map((r) =>
    `<tr><td>${r.seq}</td><td>${r.ts}</td><td class="${r.cls}">${esc(r.topic)}</td>` +
    `<td>${esc(r.agent)}</td><td>${esc(r.detail)}</td></tr>`).join("")
    : '<tr><td colspan="5" class="dim">no events yet</td></tr>';
}

function renderAgents(s) {
  const all = [...(s.agents || []),
    ...(s.agents_planned || []).map((n) => ({
      name: n, job: "", state: "PHASE 6", restarts: "—", inbox: "—",
      p99_ms: "—", beat_s: "—", up: "—" }))];
  if (!changed("agents", all)) return;
  $("agents-body").innerHTML = all.map((a) =>
    `<tr><td>${a.name}</td><td class="dim">${esc(a.job)}</td>` +
    `<td class="${a.state === "RUN" ? "up" : a.state === "PHASE 6" ? "dim" : "dn"}">${esc(a.state)}</td>` +
    `<td>${a.up}</td><td>${a.restarts}</td><td>${a.inbox}</td>` +
    `<td>${typeof a.p99_ms === "number" ? a.p99_ms.toFixed(1) + " ms" : a.p99_ms}</td>` +
    `<td>${typeof a.beat_s === "number" ? a.beat_s.toFixed(1) + " s" : a.beat_s}</td></tr>`
  ).join("");
  $("busrates").textContent = Object.entries(s.bus_rates || {})
    .map(([t, n]) => `${t} ${n}`).join(" · ") || "—";
}

function renderStatus(s) {
  const bar = $("statusbar");
  const pos = s.position.open ?
    `position OPEN ${s.position.qty} ${s.position.instrument}` : "FLAT";
  const wd = s.watchdog || {};
  const stale = wd.stale_s === null || wd.stale_s === undefined ? "—" :
    (wd.stale_s > 5 ? `${wd.stale_s}s STALE` : `${wd.stale_s}s`);
  bar.textContent =
    `${s.killed ? (wd.squared_off ? "■ SQUARED OFF" : "■ KILLED") :
      s.demo ? "● DEMO feed" : s.header.connected ? "● feed connected" :
      "○ feed idle"}${(!s.demo && s.shadow) ? " · SHADOW" : ""}` +
    `  |  ticks/s ${s.header.ticks_per_s ?? 0}` +
    `  |  feed age ${stale} · reconnects ${wd.reconnects ?? 0}` +
    `  |  latency ${wd.feed_latency_ms ?? "—"} ms` +
    `  |  ${s.mode} mode  |  ${pos}` +
    `  |  trades ${s.signals.today.trades}/${s.signals.today.max}` +
    `  |  agents ${(s.agents || []).length} running` +
    `  |  square-off ${wd.square_off_at ?? s.settings.square_off}` +
    `  |  ${s.ts} IST  |  REPLAY ${s.settings.replay ? "ON" : "OFF"}`;
  bar.className = "card mono" +
    (s.killed ? " err" : (s.demo || (wd.stale_s ?? 0) > 5) ? " warn" : "");
}

/* ───────────────────────── UI events ───────────────────────── */
document.querySelectorAll("#tabs button").forEach((b) => b.onclick = () => {
  document.querySelectorAll("#tabs button").forEach((x) => x.classList.toggle("on", x === b));
  document.querySelectorAll(".tabbody").forEach((t) => { t.style.display = "none"; });
  $("tab-" + b.dataset.tab).style.display = "";
});

$("m-auto").onclick = () => cmd("set_mode", { mode: "AUTO" });
$("m-manual").onclick = () => cmd("set_mode", { mode: "MANUAL" });
$("idxsel").onchange = (e) => cmd("set_index", { index: e.target.value });

let lotsLocal = 1;
function bumpLots(d) {
  lotsLocal = Math.max(1, Math.min(10, lotsLocal + d));
  cmd("set_lots", { lots: lotsLocal });
}
$("lot-minus").onclick = () => bumpLots(-1);
$("lot-plus").onclick = () => bumpLots(1);

$("exec-CE").onclick = () => cmd("execute", { side: "CE" });
$("exec-PE").onclick = () => cmd("execute", { side: "PE" });
$("exit-btn").onclick = () => cmd("exit_position");

$("kill").onclick = () => {
  if (lastSnap && lastSnap.killed) cmd("rearm");
  else $("modal").style.display = "flex";
};
$("m-cancel").onclick = () => { $("modal").style.display = "none"; };
$("m-confirm").onclick = () => {
  $("modal").style.display = "none";
  cmd("kill");
  document.querySelector('#tabs button[data-tab="trade"]').click();
};

document.querySelectorAll("[data-broker-save]").forEach((b) => b.onclick = () => {
  const which = b.dataset.brokerSave;
  const key = $(which === "upstox" ? "up-key" : "gr-key").value;
  const secret = $(which === "upstox" ? "up-secret" : "gr-secret").value;
  const extra = which === "groww" ? { totp_secret: $("gr-totp").value } :
    { redirect_uri: $("up-redirect").value };
  cmd("broker_save", { broker: which, api_key: key, api_secret: secret, ...extra });
});
document.querySelectorAll("[data-broker-connect]").forEach((b) => b.onclick =
  () => cmd("broker_connect", {
    broker: b.dataset.brokerConnect,
    code: $("up-code") ? $("up-code").value.trim() : "",
  }));
document.querySelectorAll("[data-broker-disconnect]").forEach((b) => b.onclick =
  () => cmd("broker_disconnect", { broker: b.dataset.brokerDisconnect }));

$("set-save").onclick = () => cmd("save_settings", {
  changes: {
    max_trades_per_day: Number($("set-maxtrades").value),
    max_daily_loss_inr: Number($("set-maxloss").value),
    sl_streak_stop: Number($("set-slstreak").value),
    stale_feed_s: Number($("set-stale").value),
    reconnect_stale_s: Number($("set-reconnect").value),
    signal_ttl_candles: Number($("set-ttl").value),
    time_stop_candles: Number($("set-timestop").value),
  },
});

$("shadow-toggle").onclick = () =>
  cmd("save_settings", { changes: { shadow: !(lastSnap && lastSnap.settings.shadow) } });

$("jfilter").oninput = (e) => { journalFilter = e.target.value.trim();
  if (lastSnap) { delete cache["journal"]; renderJournal(lastSnap); } };
$("jexport").onclick = () => {
  cmd("journal_export");                 // full SQLite dump → data dir
  const rows = (lastSnap && lastSnap.journal) || [];
  const csv = ["seq,ts,topic,agent,detail",
    ...rows.map((r) => [r.seq, r.ts, r.topic, r.agent,
      `"${String(r.detail).replace(/"/g, '""')}"`].join(","))].join("\n");
  const a = document.createElement("a");
  a.href = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
  a.download = `scapler_journal_${(lastSnap && lastSnap.ts || "now").replace(/:/g, "")}.csv`;
  a.click();
  URL.revokeObjectURL(a.href);
};

/* keep lots stepper in sync with the runtime snapshot */
setInterval(() => {
  if (lastSnap && document.activeElement !== $("lot-minus") &&
      document.activeElement !== $("lot-plus"))
    lotsLocal = lastSnap.controls.lots;
}, 1000);
