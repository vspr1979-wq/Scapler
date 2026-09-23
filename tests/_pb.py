"""Minimal protobuf wire ENCODER for tests — field numbers mirror the official
MarketDataFeedV3.proto vendored in scapler/brokers/upstox/proto/."""
from __future__ import annotations

import struct


def varint(v: int) -> bytes:
    out = bytearray()
    while True:
        b = v & 0x7F
        v >>= 7
        out.append(b | (0x80 if v else 0))
        if not v:
            return bytes(out)


def _tag(fnum: int, wt: int) -> bytes:
    return varint((fnum << 3) | wt)


def d64(fnum: int, x: float) -> bytes:
    return _tag(fnum, 1) + struct.pack("<d", x)


def i64(fnum: int, v: int) -> bytes:
    return _tag(fnum, 0) + varint(v)


def emb(fnum: int, payload: bytes) -> bytes:
    return _tag(fnum, 2) + varint(len(payload)) + payload


def strf(fnum: int, s: str) -> bytes:
    return emb(fnum, s.encode())


# LTPC { ltp=1, ltt=2, ltq=3, cp=4 }
def ltpc(ltp: float, ltt: int, ltq: int = 0, cp: float = 0.0) -> bytes:
    return d64(1, ltp) + i64(2, ltt) + i64(3, ltq) + d64(4, cp)


# Quote { bidQ=1, bidP=2, askQ=3, askP=4 }
def quote(bidq: int, bidp: float, askq: int, askp: float) -> bytes:
    return i64(1, bidq) + d64(2, bidp) + i64(3, askq) + d64(4, askp)


# OptionGreeks { delta=1, theta=2, gamma=3, … }
def greeks(delta: float, gamma: float, theta: float = 0.0) -> bytes:
    return d64(1, delta) + d64(2, theta) + d64(3, gamma)


# MarketLevel { repeated Quote bidAskQuote=1 }
def market_level(*quotes: bytes) -> bytes:
    return b"".join(emb(1, q) for q in quotes)


# MarketFullFeed { ltpc=1, marketLevel=2, optionGreeks=3, …, vtt=6, oi=7 }
def market_ff(ltpc_b: bytes, level: bytes | None = None,
              greeks_b: bytes | None = None, vtt: int = 0,
              oi: float = 0.0) -> bytes:
    b = emb(1, ltpc_b)
    if level is not None:
        b += emb(2, level)
    if greeks_b is not None:
        b += emb(3, greeks_b)
    return b + i64(6, vtt) + d64(7, oi)


# IndexFullFeed { ltpc=1 }
def index_ff(ltpc_b: bytes) -> bytes:
    return emb(1, ltpc_b)


# FirstLevelWithGreeks { ltpc=1, firstDepth=2, optionGreeks=3, vtt=4, oi=5 }
def first_level(ltpc_b: bytes, depth: bytes | None = None,
                greeks_b: bytes | None = None, vtt: int = 0,
                oi: float = 0.0) -> bytes:
    b = emb(1, ltpc_b)
    if depth is not None:
        b += emb(2, depth)
    if greeks_b is not None:
        b += emb(3, greeks_b)
    return b + i64(4, vtt) + d64(5, oi)


# Feed { ltpc=1 | fullFeed=2 | firstLevelWithGreeks=3 }
def feed_ltpc(ltpc_b: bytes) -> bytes:
    return emb(1, ltpc_b)


def feed_full(inner_full_feed: bytes) -> bytes:
    return emb(2, inner_full_feed)


def full_market(mkt: bytes) -> bytes:
    return emb(1, mkt)


def full_index(ix: bytes) -> bytes:
    return emb(2, ix)


def feed_first_level(fl: bytes) -> bytes:
    return emb(3, fl)


# FeedResponse { type=1, map<string,Feed> feeds=2, currentTs=3 }
def feed_response(entries: list[tuple[str, bytes]], typ: int = 1,
                  current_ts: int = 0) -> bytes:
    b = i64(1, typ)
    for key, feed_b in entries:
        b += emb(2, strf(1, key) + emb(2, feed_b))
    return b + i64(3, current_ts)
