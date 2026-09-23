"""CLI: python -m scapler.ui [--dev] [--demo] [--host H] [--port P].

--dev   aiohttp dev/preview server (sandbox & operator browsers)
--demo  labelled synthetic fixtures + stub broker (default with --dev)
(default, Windows) pywebview/WebView2 desktop window.
"""
import argparse
import asyncio


def main() -> None:
    ap = argparse.ArgumentParser(prog="scapler.ui")
    ap.add_argument("--dev", action="store_true",
                    help="serve the UI over HTTP+WS instead of a webview")
    ap.add_argument("--demo", action="store_true",
                    help="synthetic labelled fixture feed + stub broker")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8787)
    args = ap.parse_args()

    from .runtime import run_desktop, run_dev
    if args.dev or args.demo:
        asyncio.run(run_dev(host=args.host, port=args.port,
                            demo=True or args.demo))
    else:
        run_desktop(demo=False)


if __name__ == "__main__":
    main()
