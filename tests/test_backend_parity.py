"""Guard that the GDX and IAMC backends describe the same quantities the same way.

Both couplers read the same REMIND run through different formats, so a consumer that keys on
the ``unit`` column must not have to know which backend produced the frame. Before the unit
vocabulary was canonicalised the two disagreed on VOM (``$/MWh`` vs ``USD/MWh``), because the
couplers hardcoded units that overwrote the ones the symbol config had already declared.
"""

from pathlib import Path

import pytest

from iampypsa.couplers.remind import RemindGdxCoupler, RemindIamcCoupler
from iampypsa.io import RemindLoader
from iampypsa.io.remind_symbols import load_symbol_specs

DATA = Path(__file__).parent / "data"
GDX = DATA / "remind2pypsa_amt_filtered.gdx"
MIF = DATA / "remind_generic_amt_filtered.mif"

# The fixtures are filtered to these regions/years; 2090 is the earliest both backends carry.
REGIONS = ["CHA", "DEU", "EWN"]
YEAR = 2090

pytestmark = pytest.mark.skipif(not (GDX.exists() and MIF.exists()), reason="fixtures missing")


def _units_by_parameter(coupler) -> dict[str, set[str]]:
    """Return ``{parameter: {unit, ...}}`` from a coupler's cost table."""
    costs = coupler.extract_cost_parameters(YEAR)
    return {param: set(grp) for param, grp in costs.groupby("parameter")["unit"]}


@pytest.fixture
def gdx_units() -> dict[str, set[str]]:
    loader = RemindLoader(str(GDX))
    return _units_by_parameter(
        RemindGdxCoupler(loader, load_symbol_specs(backend="gdx"), {}, {}, model_regions=REGIONS)
    )


@pytest.fixture
def iamc_units() -> dict[str, set[str]]:
    loader = RemindLoader(str(MIF))
    return _units_by_parameter(
        RemindIamcCoupler(loader, load_symbol_specs(backend="iamc"), {}, {}, model_regions=REGIONS)
    )


def test_both_backends_cover_the_same_parameters(gdx_units, iamc_units):
    assert set(gdx_units) == set(iamc_units)


def test_both_backends_label_each_parameter_identically(gdx_units, iamc_units):
    """The regression guard: one parameter, one unit string, whichever backend produced it."""
    divergent = {
        param: (gdx_units[param], iamc_units[param])
        for param in set(gdx_units) & set(iamc_units)
        if gdx_units[param] != iamc_units[param]
    }
    assert not divergent, f"backends disagree on units (gdx, iamc): {divergent}"


def test_currency_units_use_the_canonical_token(gdx_units, iamc_units):
    """No bare ``$`` survives into the output — see UNIT_CONVERSIONS' target-side rule."""
    for units in (gdx_units, iamc_units):
        monetary = {u for group in units.values() for u in group if "$" in u or "USD" in u}
        assert monetary
        assert all(u.startswith("USD") for u in monetary), monetary
