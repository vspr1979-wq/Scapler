"""Dependency-free protobuf wire decoder for Upstox Market Data Feed V3.

Field numbers are taken verbatim from the official schema vendored at
``scapler/brokers/upstox/proto/MarketDataFeedV3.proto`` (upstox/upstox-python,
package com.upstox.marketdatafeederv3udapi.rpc.proto):

  FeedResponse { Type type=1; map<string,Feed> feeds=2; int64 currentTs=3; … }
  Feed   { oneof { LTPC ltpc=1; FullFeed fullFeed=2;
                   FirstLevelWithGreeks firstLevelWithGreeks=3; } }
  LTPC   { double ltp=1; int64 ltt=2; int64 ltq=3; double cp=4; }
  FullFeed { oneof { MarketFullFeed marketFF=1; IndexFullFeed indexFF=2; } }
  MarketFullFeed { LTPC ltpc=1; MarketLevel marketLevel=2; OptionGreeks
                   optionGreeks=3; … int64 vtt=6; double oi=7; … }
  IndexFullFeed  { LTPC ltpc=1; … }
  FirstLevelWithGreeks { LTPC ltpc=1; Quote firstDepth=2; OptionGreeks
                         optionGreeks=3; int64 vtt=4; double oi=5; … }
  Quote  { int64 bidQ=1; double bidP=2; int64 askQ=3; double askP=4; }
  OptionGreeks { double delta=1; … double gamma=3; … }

Malformed frames/entries are skipped, never raised into the hot path.
"""
from __future__ import annotations

import struct
from typing import Any

_WT_VARINT, _WT_FIXED64, _WT_LEN, _WT_FIXED32 = 0, 1, 2, 5


def _varint(buf: bytes, i: int) -> tuple[int, int]:
    val = 0
    shift = 0
    while True:
        b = buf[i]
        i += 1
        val |= (b & 0x7F) << shift
        if not b & 0x80:
            return val, i
        shift += 7


def parse(buf: bytes, start: int = 0,
          end: int | None = None) -> dict[int, list[tuple[int, Any]]]:
    end = len(buf) if end is None else end
    out: dict[int, list[tuple[int, Any]]] = {}
    i = start
    while i < end:
        key, i = _varint(buf, i)
        fnum, wt = key >> 3, key & 7
        if wt == _WT_VARINT:
            val, i = _varint(buf, i)
        elif wt == _WT_FIXED64:
            val = buf[i:i + 8]
            i += 8
        elif wt == _WT_LEN:
            ln, i = _varint(buf, i)
            val = buf[i:i + ln]
            i += ln
        elif wt == _WT_FIXED32:
            val = buf[i:i + 4]
            i += 4
        else:
            raise ValueError(f"bad wire type {wt} at {i}")
        out.setdefault(fnum, []).append((wt, val))
    return out


def _double(v: Any) -> float:
    if isinstance(v, bytes):
        return struct.unpack("<d", v[:8])[0] if len(v) >= 8 else 0.0
    return float(v)


def f_double(fs: dict, num: int, default: float = 0.0) -> float:
    e = fs.get(num)
    return _double(e[0][1]) if e else default


def f_int(fs: dict, num: int, default: int = 0) -> int:
    e = fs.get(num)
    if not e:
        return default
    wt, v = e[0]
    return v if wt == _WT_VARINT else default


def f_subs(fs: dict, num: int) -> list[bytes]:
    return [v for wt, v in fs.get(num, []) if wt == _WT_LEN]


def f_str(fs: dict, num: int, default: str = "") -> str:
    s = f_subs(fs, num)
    return s[0].decode("utf-8", "replace") if s else default


def _ltpc(buf: bytes) -> dict:
    fs = parse(buf)
    return {"ltp": f_double(fs, 1), "ltt": f_int(fs, 2),
            "ltq": f_int(fs, 3), "cp": f_double(fs, 4)}


def _quote(buf: bytes) -> dict:
    fs = parse(buf)
    return {"bidQ": f_int(fs, 1), "bidP": f_double(fs, 2),
            "askQ": f_int(fs, 3), "askP": f_double(fs, 4)}


def _greeks(buf: bytes) -> dict:
    fs = parse(buf)
    return {"delta": f_double(fs, 1), "gamma": f_double(fs, 3)}


def _market_full(buf: bytes) -> dict:
    fs = parse(buf)
    out = {"ltpc": _ltpc(subs[0]) if (subs := f_subs(fs, 1)) else None,
           "vtt": f_int(fs, 6), "oi": f_double(fs, 7)}
    g = f_subs(fs, 3)
    out["greeks"] = _greeks(g[0]) if g else None
    lvl = f_subs(fs, 2)
    if lvl:
        quotes = f_subs(parse(lvl[0]), 1)
        out["depth0"] = _quote(quotes[0]) if quotes else None
    else:
        out["depth0"] = None
    return out


def _first_level(buf: bytes) -> dict:
    fs = parse(buf)
    out = {"ltpc": _ltpc(subs[0]) if (subs := f_subs(fs, 1)) else None,
           "vtt": f_int(fs, 4), "oi": f_double(fs, 5)}
    d = f_subs(fs, 2)
    out["depth0"] = _quote(d[0]) if d else None
    g = f_subs(fs, 3)
    out["greeks"] = _greeks(g[0]) if g else None
    return out


def decode_feed(buf: bytes) -> dict | None:
    """→ {ltp, ltt, bid, ask, oi, vtt, delta, gamma} or None."""
    try:
        fs = parse(buf)
    except (ValueError, IndexError):
        return None
    inner: dict | None = None
    if (s := f_subs(fs, 1)):                      # Feed.ltpc
        inner = {"ltpc": _ltpc(s[0]), "depth0": None, "greeks": None,
                 "vtt": 0, "oi": 0.0}
    elif (s := f_subs(fs, 2)):                   # Feed.fullFeed
        ff = parse(s[0])
        if (m := f_subs(ff, 1)):
            inner = _market_full(m[0])
        elif (ix := f_subs(ff, 2)):
            ifs = parse(ix[0])
            l = f_subs(ifs, 1)
            inner = {"ltpc": _ltpc(l[0]) if l else None, "depth0": None,
                     "greeks": None, "vtt": 0, "oi": 0.0}
    elif (s := f_subs(fs, 3)):                   # Feed.firstLevelWithGreeks
        inner = _first_level(s[0])
    if not inner or not inner.get("ltpc"):
        return None
    l = inner["ltpc"]
    d0 = inner.get("depth0") or {}
    g = inner.get("greeks") or {}
    return {"ltp": l["ltp"], "ltt": l["ltt"], "bid": d0.get("bidP", 0.0),
            "ask": d0.get("askP", 0.0), "oi": inner.get("oi", 0.0),
            "vtt": inner.get("vtt", 0), "delta": g.get("delta"),
            "gamma": g.get("gamma")}


def decode_feed_response(buf: bytes) -> list[tuple[str, dict]]:
    """FeedResponse → [(feed_key, decoded feed)]; market_info/empty → []."""
    try:
        fs = parse(buf)
    except (ValueError, IndexError):
        return []
    if f_int(fs, 1, 0) == 2:                     # Type.market_info → ignore
        return []
    out = []
    for entry in f_subs(fs, 2):                  # map<string, Feed>
        try:
            ef = parse(entry)
            key = f_str(ef, 1)
            v = f_subs(ef, 2)
            if not key or not v:
                continue
            feed = decode_feed(v[0])
            if feed:
                out.append((key, feed))
        except (ValueError, IndexError):
            continue
    return out
