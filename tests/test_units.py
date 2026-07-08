"""Tests for the central unit-conversion table and resolver."""

from __future__ import annotations

import pytest

from iampypsa.units import (
    HOURS_PER_YEAR,
    TONNE_C_TO_TONNE_CO2,
    UNIT_CONVERSIONS,
    unit_factor,
)


def test_identity_is_one_without_a_table_entry():
    assert unit_factor("p.u.", "p.u.") == 1.0
    assert ("yr", "yr") not in UNIT_CONVERSIONS  # identity needs no row


def test_known_pairs_match_remind_conventions():
    assert unit_factor("$/tC", "$/tCO2") == pytest.approx(TONNE_C_TO_TONNE_CO2)
    assert unit_factor("TW", "MW") == 1e6
    assert unit_factor("TWa", "MWh") == pytest.approx(1e6 * HOURS_PER_YEAR)
    assert unit_factor("T$/TWa", "$/MWh") == pytest.approx(1e6 / HOURS_PER_YEAR)
    assert unit_factor("p.u.", "%/yr") == 100.0


def test_unknown_pair_fails_loud():
    with pytest.raises(KeyError, match="No unit conversion"):
        unit_factor("furlongs", "MWh")
