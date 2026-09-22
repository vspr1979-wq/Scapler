"""F5 acceptance: 11 strikes only, re-center at ±2 steps, per-side labels,
delta-band pick with liquidity guard."""
import pytest

from scapler.core.messages import OptionType as OT
from scapler.strategy.strikes import (
    StrikeQuote, atm_strike, build_window, moneyness_label, needs_recenter,
    select_strike, spread_ticks,
)

BAND = (0.45, 0.60)


def q(strike, delta=None, bid=1.0, ask=1.05, vol=1000.0, tick=0.05):
    return StrikeQuote(strike=strike, delta=delta, bid=bid, ask=ask,
                       tick=tick, volume_1m=vol)


def test_window_is_exactly_11_strikes():
    w = build_window(51203.0, 100.0)
    assert w.center == 51200.0
    assert w.strikes == tuple(50700.0 + 100.0 * i for i in range(11))


def test_window_rejects_wrong_radius():
    from scapler.strategy.strikes import Window
    with pytest.raises(ValueError):
        Window(center=100.0, step=50.0, strikes=(50.0, 100.0, 150.0))


def test_atm_rounding():
    assert atm_strike(24512.3, 50.0) == 24500.0
    assert atm_strike(24525.0, 50.0) == 24550.0 or atm_strike(24525.0, 50.0) == 24500.0


def test_recenter_at_two_steps_only():
    w = build_window(51203.0, 100.0)
    assert not needs_recenter(w, 51399.0)      # 1.99 steps
    assert needs_recenter(w, 51400.0)          # exactly 2 steps
    assert needs_recenter(w, 51000.0)          # downside too


def test_moneyness_labels_flip_per_side():
    w = build_window(51203.0, 100.0)
    assert moneyness_label(51000.0, w, OT.CE) == "ITM2"
    assert moneyness_label(51000.0, w, OT.PE) == "OTM2"
    assert moneyness_label(51400.0, w, OT.PE) == "ITM2"
    assert moneyness_label(51200.0, w, OT.CE) == "ATM"
    assert moneyness_label(51700.0, w, OT.CE) == "OTM5"


def test_pick_prefers_in_band_nearest_atm():
    w = build_window(51203.0, 100.0)
    quotes = [q(51100.0, +0.62), q(51200.0, +0.53), q(51300.0, +0.44)]
    assert select_strike(OT.CE, w, quotes, BAND, 2)[0] == 51200.0   # 0.62 out of band


def test_pick_two_candidates_nearest_atm_wins():
    w = build_window(51203.0, 100.0)
    quotes = [q(51100.0, +0.58), q(51200.0, +0.53)]
    assert select_strike(OT.CE, w, quotes, BAND, 2)[0] == 51200.0


def test_illiquid_atm_falls_back_to_liquid_itm():
    w = build_window(51203.0, 100.0)
    quotes = [q(51100.0, +0.58, bid=1.0, ask=1.05),      # 1 tick
              q(51200.0, +0.53, bid=1.0, ask=1.50)]      # 10 ticks → rejected
    strike, delta, sp = select_strike(OT.CE, w, quotes, BAND, 2)
    assert strike == 51100.0 and delta == 0.58 and sp == 1


def test_pe_uses_negative_delta():
    w = build_window(51203.0, 100.0)
    # +0.52 at ATM is a CE-style delta → must NOT be picked for PE;
    # -0.52 at OTM1 is the PE-side candidate
    quotes = [q(51200.0, +0.52), q(51300.0, -0.52)]
    assert select_strike(OT.PE, w, quotes, BAND, 2)[0] == 51300.0
    # and mirrored: CE must take the +delta one
    assert select_strike(OT.CE, w, quotes, BAND, 2)[0] == 51200.0


def test_no_candidates_falls_back_to_atm():
    w = build_window(51203.0, 100.0)
    quotes = [q(51200.0, None), q(51300.0, +0.10)]
    strike, delta, _ = select_strike(OT.CE, w, quotes, BAND, 2)
    assert strike == 51200.0 and delta is None


def test_dead_volume_rejected():
    w = build_window(51203.0, 100.0)
    quotes = [q(51200.0, +0.53, vol=0.0)]
    assert select_strike(OT.CE, w, quotes, BAND, 2)[0] == 51200.0  # ATM fallback


def test_spread_ticks():
    assert spread_ticks(q(1.0, bid=148.20, ask=148.25, tick=0.05)) == 1
