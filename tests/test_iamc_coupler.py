"""The generic IAMC coupler, against a fixture containing no IAM-specific variables.

This is the proof that the package is IAM-generic rather than REMIND with a rename: nothing in
``generic_iamc.mif`` matches a REMIND variable name, and no code outside ``models/iamc`` runs.
"""

from pathlib import Path

import pytest

from iampypsa import build_coupler
from iampypsa.models.iamc import IamcCoupler

MIF = Path(__file__).parent / "data" / "generic_iamc.mif"
REGION_MAP = {"EUR": ["DE", "FR"], "ASI": ["CN"]}
EJ_PER_YR_TO_MWH = 1e18 / 3.6e9


@pytest.fixture
def coupler() -> IamcCoupler:
    return build_coupler(MIF, model="iamc", region_map=REGION_MAP, config={})


def test_build_coupler_selects_the_generic_coupler(coupler):
    assert isinstance(coupler, IamcCoupler)
    assert coupler.loader.backend == "iamc"
    assert sorted(coupler.model_regions) == ["ASI", "EUR"]


def test_regional_demand_labels_sectors_and_converts_units(coupler):
    demand = coupler.build_regional_demand()

    assert set(demand["sector"]) == {"AC", "industry", "EV_pass", "electrolysis"}
    assert set(demand["unit"]) == {"MWh"}
    row = demand.query("year == 2030 and region == 'EUR' and sector == 'AC'")["value"]
    assert row.iloc[0] == pytest.approx(4 * EJ_PER_YR_TO_MWH)


def test_cost_parameters_cover_the_declared_vocabulary(coupler):
    costs = coupler.extract_cost_parameters(2030)

    assert set(costs["parameter"]) == {
        "investment", "lifetime", "FOM", "VOM", "efficiency", "fuel", "CO2 intensity",
    }
    units = costs.drop_duplicates(["parameter", "unit"]).set_index("parameter")["unit"]
    assert units["investment"] == "USD/MW"
    assert units["FOM"] == "%/yr"
    assert units["CO2 intensity"] == "t_CO2/MWh_th"

    capex = costs.query("region == 'EUR' and technology == 'coal-pulverised' and parameter == 'investment'")
    assert capex["value"].iloc[0] == pytest.approx(1600 * 1e3)  # US$2017/kW -> USD/MW
    eff = costs.query("region == 'EUR' and technology == 'gas-ccgt' and parameter == 'efficiency'")
    assert eff["value"].iloc[0] == pytest.approx(0.58)  # % -> p.u.


def test_fuel_prices_are_broadcast_onto_the_burning_technologies(coupler):
    costs = coupler.extract_cost_parameters(2030)
    fuel = costs[costs["parameter"] == "fuel"].set_index(["region", "technology"])["value"]

    # coal-pulverised burns coal-fuel: 2 US$2017/GJ -> 7.2 USD/MWh
    assert fuel[("EUR", "coal-pulverised")] == pytest.approx(2 * 3.6)
    assert fuel[("ASI", "gas-ccgt")] == pytest.approx(5 * 3.6)
    # solar-pv burns nothing priced
    assert fuel[("EUR", "solar-pv")] == pytest.approx(0.0)


def test_declared_fallback_fills_a_missing_emission_factor(coupler):
    costs = coupler.extract_cost_parameters(2030)
    biomass = costs.query("technology == 'biomass-igcc' and parameter == 'CO2 intensity'")

    assert not biomass.empty
    assert (biomass["value"] == 0.0).all()


def test_inherited_builders_work_unchanged(coupler):
    """Every concrete builder on Coupler is IAM-agnostic, so the generic coupler gets them free."""
    prices = coupler.build_co2_prices(years=[2030, 2050]).set_index(["region", "year"])["value"]
    assert prices[("EUR", 2030)] == pytest.approx(50.0)
    assert prices[("ASI", 2050)] == pytest.approx(120.0)

    rates = coupler.build_discount_rates(2030)
    assert rates["ASI"] == pytest.approx(0.07)
