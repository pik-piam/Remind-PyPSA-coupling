"""Tests for the central unit-conversion table and resolver."""

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
    assert unit_factor("USD/tC", "USD/tCO2") == pytest.approx(TONNE_C_TO_TONNE_CO2)
    assert unit_factor("TW", "MW") == 1e6
    assert unit_factor("TWa", "MWh") == pytest.approx(1e6 * HOURS_PER_YEAR)
    assert unit_factor("TUSD/TWa", "USD/MWh") == pytest.approx(1e6 / HOURS_PER_YEAR)
    assert unit_factor("p.u.", "%/yr") == 100.0


def test_currency_targets_use_one_token():
    """Every monetary target unit says USD, so the two backends cannot label the same
    parameter differently."""
    monetary = [to_unit for _, to_unit in UNIT_CONVERSIONS if "$" in to_unit or "USD" in to_unit]
    assert monetary  # guard against the filter silently matching nothing
    assert all(u.startswith("USD") for u in monetary), monetary


def test_dollar_survives_only_in_units_the_mif_owns():
    """``$`` is allowed only in ``US$<year>`` source units, which must match the mif's own
    ``Unit`` column verbatim. Units we declare ourselves (the GDX map) say ``USD``."""
    offenders = [
        unit
        for pair in UNIT_CONVERSIONS
        for unit in pair
        if "$" in unit and not unit.startswith("US$")
    ]
    assert not offenders, offenders


def test_unknown_pair_fails_loud():
    with pytest.raises(KeyError, match="No unit conversion"):
        unit_factor("furlongs", "MWh")
