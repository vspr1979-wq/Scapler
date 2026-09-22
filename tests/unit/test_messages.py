import msgspec
import pytest

from scapler.core import config
from scapler.core.messages import (
    COALESCED_TOPICS, PRIORITY_TOPICS, OrderIntent, OrderRequest, InstrumentKey,
    Tick, Topic, Envelope,
)


def test_order_intent_has_no_raw_side():
    assert {i.name for i in OrderIntent} == {"BUY_TO_OPEN", "SELL_TO_CLOSE"}


def test_messages_are_frozen():
    t = Tick(key="K", exch_ts_ns=1, ltp=2.0)
    with pytest.raises(AttributeError):
        t.ltp = 3.0
    ik = InstrumentKey(exchange="NSE_FO", token="1", symbol="S")
    with pytest.raises(AttributeError):
        ik.token = "2"


def test_order_request_carries_intent_not_side():
    r = OrderRequest(
        intent=OrderIntent.BUY_TO_OPEN,
        instrument=InstrumentKey(exchange="NSE_FO", token="9", symbol="BANKNIFTY29SEP26C51200"),
        qty=30, client_id="SCP-260929-0001", ts_mono=0,
    )
    assert not hasattr(r, "side")
    assert r.intent is OrderIntent.BUY_TO_OPEN


def test_priority_and_coalesced_sets():
    assert Topic.ORDER_REQ in PRIORITY_TOPICS
    assert Topic.KILL_SWITCH in PRIORITY_TOPICS
    assert Topic.TICK_RAW in COALESCED_TOPICS
    assert Topic.CANDLE_CLOSED not in COALESCED_TOPICS


def test_envelope_json_roundtrip():
    env = Envelope(seq=1, topic=Topic.TICK_RAW, key="K", ts_mono=5,
                   payload=Tick(key="K", exch_ts_ns=5, ltp=1.5))
    blob = msgspec.json.encode(env)
    back = msgspec.json.decode(blob, type=Envelope)
    assert back.payload["ltp"] == 1.5   # Any decodes to dict; agents decode typed


def test_feed_key_format():
    ik = InstrumentKey(exchange="NSE_FO", token="12345", symbol="X")
    assert ik.feed_key == "NSE_FO|12345"


def test_settings_defaults_match_plan():
    s = config.Settings()
    assert s.entry_window == ("09:17", "15:15")
    assert s.square_off == "15:20"
    assert s.max_trades_per_day == 3
    assert s.targets == config.Targets(5, 12, 20, 8)
    assert s.setup.atr_min["BANKNIFTY"] == 25.0
    assert s.signal_ttl_candles == 3
    assert s.lot_multiplier == 1


def test_settings_save_load_roundtrip(tmp_path):
    p = tmp_path / "settings.json"
    s = config.Settings(index="NIFTY", lot_multiplier=4, mode="AUTO")
    config.save(s, p)
    back = config.load(p)
    assert back == s


def test_settings_load_missing_file_returns_defaults(tmp_path):
    assert config.load(tmp_path / "nope.json") == config.Settings()
