"""UI transports.

Production (Windows desktop): pywebview + WebView2. JS→Py via ``js_api.cmd``;
Py→JS via ``evaluate_js("window.__snap(…)")`` at the UIAgent snapshot rate.
No HTTP server, no CORS (plan §7).

Development/preview: aiohttp serves the SAME static frontend plus ``/ws``
(server→client snapshots) and ``POST /api/cmd`` (client→server commands).
Dev transport is for operator machines and the sandbox preview only — the
packaged app never opens a port.
"""
from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

import orjson
from aiohttp import web

from ..core.messages import Topic

log = logging.getLogger(__name__)
WEB_DIR = Path(__file__).parent / "web"


# ────────────────────────── dev/preview transport ──────────────────────────
async def start_dev_server(runtime, host: str = "0.0.0.0",
                           port: int = 8787) -> None:
    """Serves the frontend + WS snapshots until cancelled. Blocks."""
    from ..core.bus import Inbox

    async def index(_req):
        return web.FileResponse(WEB_DIR / "index.html")

    async def cmd(req):
        body = await req.json()
        out = runtime.ui.handle_command(body.get("name", ""),
                                        body.get("args") or {})
        return web.json_response(out)

    async def ws(req):
        sock = web.WebSocketResponse(heartbeat=15.0)
        await sock.prepare(req)
        inbox = Inbox()
        runtime.bus.subscribe(inbox, Topic.UI_SNAPSHOT)
        task = asyncio.get_running_loop().create_task(_pump(sock, inbox))
        try:
            async for msg in sock:                 # client → server commands
                if msg.type == web.WSMsgType.TEXT:
                    try:
                        d = json.loads(msg.data)
                        if d.get("t") == "cmd":
                            out = runtime.ui.handle_command(
                                d.get("name", ""), d.get("args") or {})
                            await sock.send_str(orjson.dumps(
                                {"t": "cmd_result", **out}).decode())
                    except Exception as e:
                        log.warning("ws cmd error: %s", e)
                elif msg.type == web.WSMsgType.ERROR:
                    break
        finally:
            task.cancel()
        return sock

    async def _pump(sock, inbox):
        try:
            while True:
                env = await inbox.get()
                if env.topic == Topic.UI_SNAPSHOT and not sock.closed:
                    await sock.send_str(orjson.dumps(
                        {"t": "snap", "snap": env.payload}).decode())
        except (asyncio.CancelledError, ConnectionResetError):
            raise

    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_post("/api/cmd", cmd)
    app.router.add_get("/ws", ws)
    app.router.add_static("/", WEB_DIR)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    log.info("scapler dev UI on http://%s:%d", host, port)
    print(f"scapler dev UI on http://{host}:{port}  (demo={runtime.demo})")
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await runner.cleanup()


# ────────────────────────── pywebview transport ──────────────────────────
def run_webview(runtime) -> None:                    # pragma: no cover (Win)
    """Windows desktop entry: asyncio loop on a worker thread, pywebview on
    the main thread (WebView2 ships with Win11)."""
    import webview

    loop = asyncio.new_event_loop()
    ready = asyncio.Event()

    def push(snap: dict) -> None:
        js = orjson.dumps(snap).decode()
        try:
            window.evaluate_js(f"window.__snap({js})")
        except Exception:
            pass                                     # window closing

    class Api:
        def cmd(self, name: str, args_json: str = "{}") -> str:
            try:
                args = json.loads(args_json or "{}")
            except json.JSONDecodeError:
                args = {}
            fut = asyncio.run_coroutine_threadsafe(_dispatch(name, args), loop)
            return json.dumps(fut.result(timeout=5.0))

    async def _dispatch(name, args):
        return runtime.ui.handle_command(name, args)

    async def _main():
        runtime.build(push_cb=push)
        await runtime.start()
        ready.set()
        while True:
            await asyncio.sleep(3600)

    def _thread():
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_main())

    import threading
    t = threading.Thread(target=_thread, daemon=True)
    t.start()

    window = webview.create_window(
        "SCAPLER — index options scalper", str(WEB_DIR / "index.html"),
        js_api=Api(), width=1320, height=920, min_size=(1100, 760),
        background_color="#0d1117")
    webview.start(gui="edgechromium")                # WebView2
    loop.call_soon_threadsafe(loop.stop)
