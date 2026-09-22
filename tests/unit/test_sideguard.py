"""F2 acceptance: adversarial order shapes vs SideGuard — 100% reject of
anything that is not 'buy an option' or 'close part/all of an existing long'."""
import pytest

from scapler.core.messages import (
    InstrumentKey, OrderIntent, OrderRequest, OptionType,
)
from scapler.strategy.sideguard import SideGuardError, check

CE = InstrumentKey(exchange="NSE_FO", token="9", symbol="BANKNIFTY29SEP26C51200",
                   strike=51200.0, option_type=OptionType.CE)
CE_OTHER = InstrumentKey(exchange="NSE_FO", token="10",
                         symbol="BANKNIFTY29SEP26C51300", strike=51300.0,
                         option_type=OptionType.CE)
FUT = InstrumentKey(exchange="NSE_FO", token="7", symbol="BANKNIFTY29SEP26FUT")
EQ = InstrumentKey(exchange="NSE_EQ", token="5", symbol="RELIANCE")


def req(intent, ik, qty):
    return OrderRequest(intent=intent, instrument=ik, qty=qty,
                        client_id="SCP-260929-0001", ts_mono=0)


def test_buy_option_ok():
    check(req(OrderIntent.BUY_TO_OPEN, CE, 30), open_qty=0, lot_size=30)


def test_sell_to_close_full_and_whole_lots_ok():
    check(req(OrderIntent.SELL_TO_CLOSE, CE, 30), open_qty=30, lot_size=30)
    check(req(OrderIntent.SELL_TO_CLOSE, CE, 60), open_qty=90, lot_size=30)


def test_partial_lot_sell_rejected():
    with pytest.raises(SideGuardError) as e:
        check(req(OrderIntent.SELL_TO_CLOSE, CE, 15), open_qty=30, lot_size=30)
    assert e.value.code == "SG_BAD_QTY"


@pytest.mark.parametrize("ik", [FUT, EQ])
def test_non_option_rejected(ik):
    with pytest.raises(SideGuardError) as e:
        check(req(OrderIntent.BUY_TO_OPEN, ik, 30), 0, 30)
    assert e.value.code == "SG_NON_OPTION"


def test_sell_without_long_is_sell_to_open_rejected():
    with pytest.raises(SideGuardError) as e:
        check(req(OrderIntent.SELL_TO_CLOSE, CE, 30), open_qty=0, lot_size=30)
    assert e.value.code == "SG_SELL_WITHOUT_LONG"


def test_sell_exceeding_long_rejected():
    with pytest.raises(SideGuardError) as e:
        check(req(OrderIntent.SELL_TO_CLOSE, CE, 60), open_qty=30, lot_size=30)
    assert e.value.code == "SG_SELL_EXCEEDS_QTY"


def test_sell_of_instrument_without_position_rejected():
    # long CE 51200 exists; selling CE 51300 would be a naked short
    with pytest.raises(SideGuardError) as e:
        check(req(OrderIntent.SELL_TO_CLOSE, CE_OTHER, 30), open_qty=0, lot_size=30)
    assert e.value.code == "SG_SELL_WITHOUT_LONG"


@pytest.mark.parametrize("qty", [0, -30, 15, 31])
def test_bad_qty_rejected(qty):
    with pytest.raises(SideGuardError) as e:
        check(req(OrderIntent.BUY_TO_OPEN, CE, qty), 0, 30)
    assert e.value.code == "SG_BAD_QTY"


def test_adversarial_matrix_only_legal_flows_pass():
    rejected = 0
    for intent in OrderIntent:
        for ik in (CE, CE_OTHER, FUT, EQ):
            for qty in (0, 15, 30, 60, 90):
                for open_qty in (0, 30, 60):
                    try:
                        check(req(intent, ik, qty), open_qty, 30)
                    except SideGuardError:
                        rejected += 1
                        continue
                    # everything that passes must be a legal long-premium flow
                    assert ik.option_type is not None
                    assert qty % 30 == 0 and qty > 0
                    if intent is OrderIntent.SELL_TO_CLOSE:
                        assert 0 < qty <= open_qty
    assert rejected >= 50
