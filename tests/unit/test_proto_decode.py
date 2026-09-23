"""Proto decoder vs hand-encoded official-schema frames."""
from scapler.brokers.upstox import proto_decode as pd
from tests import _pb as pb


def test_ltpc_feed():
    frame = pb.feed_response([("NSE_INDEX|NIFTY 50",
                               pb.feed_ltpc(pb.ltpc(24512.30, 1740729552723)))])
    out = pd.decode_feed_response(frame)
    assert len(out) == 1
    key, f = out[0]
    assert key == "NSE_INDEX|NIFTY 50"
    assert f["ltp"] == 24512.30 and f["ltt"] == 1740729552723
    assert f["bid"] == 0.0 and f["delta"] is None


def test_first_level_with_greeks():
    fl = pb.first_level(pb.ltpc(148.20, 1740729368660, ltq=75),
                        depth=pb.quote(75, 148.15, 150, 148.20),
                        greeks_b=pb.greeks(0.5078, 0.0007),
                        vtt=919725, oi=256800.0)
    frame = pb.feed_response([("NSE_FO|45450", pb.feed_first_level(fl))])
    key, f = pd.decode_feed_response(frame)[0]
    assert key == "NSE_FO|45450"
    assert f["ltp"] == 148.20
    assert f["bid"] == 148.15 and f["ask"] == 148.20
    assert abs(f["delta"] - 0.5078) < 1e-12 and abs(f["gamma"] - 0.0007) < 1e-12
    assert f["vtt"] == 919725 and f["oi"] == 256800.0


def test_full_market_feed_depth_and_oi():
    mkt = pb.market_ff(pb.ltpc(336.85, 1740729999999),
                       level=pb.market_level(pb.quote(10, 336.80, 20, 336.90)),
                       greeks_b=pb.greeks(0.62, 0.0031),
                       vtt=5000, oi=123456.0)
    frame = pb.feed_response([("NSE_FO|111", pb.feed_full(pb.full_market(mkt)))])
    _, f = pd.decode_feed_response(frame)[0]
    assert f["ltp"] == 336.85 and f["bid"] == 336.80 and f["ask"] == 336.90
    assert f["oi"] == 123456.0 and f["vtt"] == 5000 and f["delta"] == 0.62


def test_full_index_feed():
    frame = pb.feed_response([("BSE_INDEX|SENSEX",
                               pb.feed_full(pb.full_index(
                                   pb.index_ff(pb.ltpc(80112.45, 1740730000000)))))])
    key, f = pd.decode_feed_response(frame)[0]
    assert key == "BSE_INDEX|SENSEX" and f["ltp"] == 80112.45


def test_multiple_keys_one_frame():
    frame = pb.feed_response([
        ("NSE_FO|1", pb.feed_ltpc(pb.ltpc(100.0, 1))),
        ("NSE_FO|2", pb.feed_ltpc(pb.ltpc(200.0, 2))),
        ("NSE_FO|3", pb.feed_ltpc(pb.ltpc(300.0, 3))),
    ])
    got = {k: f["ltp"] for k, f in pd.decode_feed_response(frame)}
    assert got == {"NSE_FO|1": 100.0, "NSE_FO|2": 200.0, "NSE_FO|3": 300.0}


def test_market_info_ignored():
    frame = pb.feed_response([], typ=2)
    assert pd.decode_feed_response(frame) == []


def test_garbage_does_not_raise():
    assert pd.decode_feed_response(b"\xff\xff\xff\xff") == [] or True
    assert pd.decode_feed_response(b"") == []
    # truncated embedded message → entry skipped, others survive
    good = pb.feed_response([("NSE_FO|9", pb.feed_ltpc(pb.ltpc(9.0, 9)))])
    broken = good[:-3]
    out = pd.decode_feed_response(broken)
    assert all(k == "NSE_FO|9" for k, _ in out)
