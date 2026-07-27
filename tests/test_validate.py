"""Tests for config-vs-GDX scenario validation."""

from pathlib import Path

import pytest

from iampypsa.io.remind_symbols import load_symbol_specs
from iampypsa.validate import validate_scenario_against_remind

GDX = Path(__file__).parent / "data" / "remind2pypsa_amt_filtered.gdx"


def test_valid_scenario_passes():
    from iampypsa.io import RemindLoader

    loader = RemindLoader(str(GDX))
    sym = load_symbol_specs(backend="gdx")
    validate_scenario_against_remind(loader, sym, ["DEU", "EWN"], [2090, 2100])  # no raise


def test_missing_region_raises():
    from iampypsa.io import RemindLoader

    loader = RemindLoader(str(GDX))
    sym = load_symbol_specs(backend="gdx")
    with pytest.raises(ValueError, match="regions"):
        validate_scenario_against_remind(loader, sym, ["DEU", "ATLANTIS"], [2090])


def test_missing_year_raises():
    from iampypsa.io import RemindLoader

    loader = RemindLoader(str(GDX))
    sym = load_symbol_specs(backend="gdx")
    with pytest.raises(ValueError, match="years"):
        validate_scenario_against_remind(loader, sym, ["DEU"], [1999])
