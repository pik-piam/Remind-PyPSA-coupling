"""Tests for config-vs-GDX scenario validation."""

from __future__ import annotations

import os

import pytest

from rpycpl.io.remind_symbols import load_symbol_specs
from rpycpl.validate import validate_scenario_against_remind

EUR_GDX = "/workspace/remind_pypsa_coupling/development_data/REMIND2PyPSAEUR.gdx"
pytestmark = pytest.mark.skipif(not os.path.exists(EUR_GDX), reason="EUR development GDX not present")


def test_valid_scenario_passes():
    from rpycpl.io import RemindLoader

    loader = RemindLoader(EUR_GDX)
    sym = load_symbol_specs()
    validate_scenario_against_remind(loader, sym, ["DEU", "FRA"], [2030, 2050])  # no raise


def test_missing_region_raises():
    from rpycpl.io import RemindLoader

    loader = RemindLoader(EUR_GDX)
    sym = load_symbol_specs()
    with pytest.raises(ValueError, match="regions"):
        validate_scenario_against_remind(loader, sym, ["DEU", "ATLANTIS"], [2030])


def test_missing_year_raises():
    from rpycpl.io import RemindLoader

    loader = RemindLoader(EUR_GDX)
    sym = load_symbol_specs()
    with pytest.raises(ValueError, match="years"):
        validate_scenario_against_remind(loader, sym, ["DEU"], [1999])
