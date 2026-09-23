"""SCAPLER desktop entry point.

Windows (packaged): double-click / `scapler` → pywebview + WebView2 window.
Development: `python -m scapler --dev [--demo] [--port P]` → browser preview.
"""
import argparse


def main() -> None:
    ap = argparse.ArgumentParser(prog="scapler")
    ap.add_argument("--dev", action="store_true",
                    help="aiohttp dev/preview server instead of a webview")
    ap.add_argument("--demo", action="store_true",
                    help="labelled synthetic fixtures + stub broker")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8787)
    ap.add_argument("--settings", default=None,
                    help="settings.json path (default: {data_dir})")
    args = ap.parse_args()

    from scapler.core import config as config_mod
    if args.dev or args.demo:
        import asyncio
        from scapler.ui.runtime import ScaplerRuntime
        from scapler.ui.transport import start_dev_server

        async def _run():
            rt = ScaplerRuntime(demo=True, settings_path=args.settings)
            rt.build()
            await rt.start()
            await start_dev_server(rt, args.host, args.port)
        asyncio.run(_run())
    else:
        from scapler.ui.runtime import run_desktop
        run_desktop(demo=args.demo, settings_path=args.settings)


if __name__ == "__main__":
    main()
