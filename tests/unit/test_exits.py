"""§6 acceptance: qty conservation, whole-lot partials, N=1 trail ladder,
no action after close, SL/time-stop/square-off sell full residual."""
import pytest

from scapler.core.config import Targets
from scapler.core.messages import ExitReason as R, InstrumentKey, OptionType
from scapler.strategy.exits import PositionExitTracker

IK = InstrumentKey(exchange="NSE_FO", token="1", symbol="BANKNIFTY29SEP26C51200",
                   strike=51200.0, option_type=OptionType.CE)
T = Targets(5, 12, 20, 8)


def trk(lots, avg=148.20):
    return PositionExitTracker(IK, lot_size=30, lots=lots, avg_price=avg, targets=T)


def sold(actions):
    return sum(a.qty for a in actions)


def test_one_lot_trail_ladder_no_partials():
    t = trk(1)
    assert t.on_tick(153.00) == []            # +4.8, below T1
    a = t.on_tick(153.20)                     # T1 hit → no sell, SL→BE
    assert a == [] and t.t1_hit and t.sl_price == 148.20 and t.qty == 30
    a = t.on_tick(160.20)                     # T2 → SL→entry+T1, still no sell
    assert a == [] and t.sl_price == 153.20 and t.qty == 30
    a = t.on_tick(168.20)                     # T3 → flat
    assert [(x.reason, x.qty) for x in a] == [(R.T3, 30)]
    assert t.closed and t.on_tick(999.0) == []


def test_one_lot_breakeven_sl_after_t1():
    t = trk(1)
    t.on_tick(153.20)                         # T1 → SL=BE
    a = t.on_tick(148.10)                     # dip to BE → SL out, no loss
    assert [(x.reason, x.qty) for x in a] == [(R.SL, 30)]
    assert t.closed


def test_one_lot_sl_before_any_target():
    t = trk(1)
    a = t.on_tick(140.20)                     # 148.20-8
    assert [(x.reason, x.qty) for x in a] == [(R.SL, 30)]


def test_four_lots_scale_out_whole_lots():
    t = trk(4)                                # 120 qty
    assert t.t1_qty == 60 and t.t2_qty == 30
    a = t.on_tick(153.20)
    assert [(x.reason, x.qty) for x in a] == [(R.T1, 60)] and t.qty == 60
    a = t.on_tick(160.20)
    assert [(x.reason, x.qty) for x in a] == [(R.T2, 30)] and t.qty == 30
    a = t.on_tick(168.20)
    assert [(x.reason, x.qty) for x in a] == [(R.T3, 30)] and t.closed


def test_two_lots():
    t = trk(2)                                # 60 qty: T1 sells 30, rem 1 lot
    assert t.t1_qty == 30 and t.t2_qty == 0
    t.on_tick(153.20)
    assert t.qty == 30 and t.sl_price == 148.20
    a = t.on_tick(168.20)
    assert [(x.reason, x.qty) for x in a] == [(R.T3, 30)]


def test_gap_tick_through_all_targets_conserves_qty():
    t = trk(4)
    acts = t.on_tick(200.00)                  # gaps T1+T2+T3 in one tick
    assert sold(acts) == 120 and t.qty == 0 and t.closed
    assert [a.reason for a in acts] == [R.T1, R.T2, R.T3]


def test_sl_after_partial_sells_residual_only():
    t = trk(4)
    t.on_tick(153.20)                         # -60
    a = t.on_tick(147.00)                     # below BE SL
    assert [(x.reason, x.qty) for x in a] == [(R.SL, 60)] and t.closed


def test_force_close_reasons():
    t = trk(2)
    t.on_tick(153.20)
    a = t.force_close(R.SQUARE_OFF, 151.0)
    assert (a.reason, a.qty) == (R.SQUARE_OFF, 30) and t.closed
    assert t.force_close(R.KILL, 150.0) is None     # nothing left


def test_qty_conservation_random_walk():
    import random
    rng = random.Random(7)
    for _ in range(300):
        lots = rng.randint(1, 10)
        t = trk(lots)
        total = 0
        price = 148.20
        for _ in range(200):
            price = max(1.0, price + rng.uniform(-1.5, 1.5))
            total += sold(t.on_tick(price))
            if t.closed:
                break
        if not t.closed:
            a = t.force_close(R.TIME_STOP, price)
            total += a.qty if a else 0
        assert total == 30 * lots
        assert t.qty == 0 and t.closed


def test_invalid_construction():
    with pytest.raises(ValueError):
        PositionExitTracker(IK, lot_size=30, lots=0, avg_price=1.0, targets=T)
