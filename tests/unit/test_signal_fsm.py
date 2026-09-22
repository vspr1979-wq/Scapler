"""F6 acceptance: one signal per side per setup episode — AUTO and MANUAL."""
from scapler.core.config import Settings
from scapler.core.messages import OptionType as OT, SignalStateEnum as S
from scapler.strategy.signal_fsm import SignalEngine


def engine(ttl=3):
    return SignalEngine(Settings(signal_ttl_candles=ttl))


def candle(e, ts, ce_valid=False, ce_broken=False, pe_valid=False, pe_broken=False):
    return e.on_candle(ts, {OT.CE: ce_valid, OT.PE: pe_valid},
                       {OT.CE: ce_broken, OT.PE: pe_broken})


def test_sixty_valid_candles_one_signal():
    e = engine()
    sigs = []
    for i in range(60):                      # relentless trend, no break
        sigs += [s for s in candle(e, i, ce_valid=True)
                 if s.new is S.SIGNALED and s.side is OT.CE]
    assert len(sigs) == 1                    # no candle spam


def test_rearm_requires_break_then_form():
    e = engine(ttl=3)
    states = [s.new for s in candle(e, 1, ce_valid=True)]
    assert states == [S.SIGNALED]
    # TTL burns out while still valid → DISARMED, and valid candles stay silent
    for i in range(2, 8):
        assert all(s.new is not S.SIGNALED for s in candle(e, i, ce_valid=True))
    assert e.fsm[OT.CE].state is S.DISARMED
    assert candle(e, 8, ce_valid=True) == []          # valid ≠ re-arm
    tr = candle(e, 9, ce_broken=True)                 # the break
    assert [s.new for s in tr] == [S.ARMED]
    tr = candle(e, 10, ce_valid=True)                 # forms again
    assert [s.new for s in tr] == [S.SIGNALED]


def test_break_while_signaled_disarms():
    e = engine()
    candle(e, 1, ce_valid=True)
    tr = candle(e, 2, ce_broken=True)
    assert [(s.old, s.new) for s in tr] == [(S.SIGNALED, S.DISARMED)]


def test_sides_independent():
    e = engine()
    tr = candle(e, 1, ce_valid=True, pe_valid=True)
    assert {s.side for s in tr if s.new is S.SIGNALED} == {OT.CE, OT.PE}


def test_held_suppresses_both_sides_then_disarms_on_flat():
    e = engine()
    candle(e, 1, ce_valid=True)
    tr = e.set_held(True)                              # fill on CE
    assert {s.new for s in tr} == {S.HELD}
    assert candle(e, 2, pe_valid=True) == []           # PE silent while held
    tr = e.set_held(False)                             # flat
    assert all(s.new is S.DISARMED for s in tr)
    assert candle(e, 3, pe_valid=True) == []           # still needs a break
    candle(e, 4, pe_broken=True)
    assert [s.new for s in candle(e, 5, pe_valid=True)] == [S.SIGNALED]


def test_ttl_zero_holds_until_break():
    e = engine(ttl=0)
    candle(e, 1, ce_valid=True)
    for i in range(2, 30):
        assert all(s.new is not S.DISARMED for s in candle(e, i, ce_valid=True))
    assert e.fsm[OT.CE].state is S.SIGNALED
    assert [s.new for s in candle(e, 30, ce_broken=True)] == [S.DISARMED]


def test_manual_no_click_no_repeat():
    e = engine()                     # MANUAL = nobody calls set_held/execute
    n = 0
    for i in range(10):
        n += sum(1 for s in candle(e, i, ce_valid=True) if s.new is S.SIGNALED)
    assert n == 1
