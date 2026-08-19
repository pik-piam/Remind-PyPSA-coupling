"""Tests for the (directly instantiable) Coupler: builders vs reference CSVs.

The data-driven checks run against the filtered GDX fixture and reference CSVs in tests/data/
(self-contained -- see tests/data/README.md for provenance; no external '/workspace/...' paths).
"""

import logging
from pathlib import Path

import pandas as pd
import pytest

from iampypsa.coupler import Coupler
from iampypsa.models.remind import RemindGdxCoupler

DATA = Path(__file__).parent / "data"
GDX = DATA / "remind2pypsa_amt_filtered.gdx"
TECH_MAPPING = DATA / "technology_mapping_example.yaml"
REGION_MAP = {"DEU": ["DE"], "EWN": ["AT", "BE", "LU", "NL"], "CHA": ["CN", "HK", "MO", "TW"]}
COUNTRIES = {"DE", "AT", "BE", "LU", "NL", "CN", "HK", "MO", "TW"}
SECTOR_WEIGHTS = {
    "AC": {"gdp": 0.6, "population": 0.4},
    "electrolysis": {"gdp": 0.7, "population": 0.3},
    "EV_pass": {"gdp": 0.3, "population": 0.7},
    "EV_freight": {"gdp": 0.5, "population": 0.5},
    "heatpump": {"gdp": 0.3, "population": 0.7},
}
YEARS = [2090, 2100]


def test_adapter_is_directly_instantiable():
    """Coupler has no abstract methods — it can be created without a subclass."""
    adapter = Coupler(loader=None, quantities={}, region_map={}, config={})
    assert isinstance(adapter, Coupler)


def test_overriding_the_cost_template_warns(caplog):
    """A subclass that overrides extract_cost_parameters skips the currency factor and the
    vocabulary rename, so it is warned about at class-definition time."""
    with caplog.at_level(logging.WARNING, logger="iampypsa.coupler"):

        class LegacyCoupler(Coupler):
            def extract_cost_parameters(self, year):
                return pd.DataFrame()

    assert "build_cost_parameters() instead" in caplog.text


def test_implementing_the_hook_does_not_warn(caplog):
    with caplog.at_level(logging.WARNING, logger="iampypsa.coupler"):

        class ModernCoupler(Coupler):
            def build_cost_parameters(self, year):
                return pd.DataFrame()

    assert caplog.text == ""


def test_finalise_applies_the_currency_factor_once():
    """The template is the only path to a cost table, so currency cannot be skipped."""
    coupler = Coupler(
        loader=None,
        quantities={},
        region_map={"DEU": ["DE"]},
        config={"currency_factor": 2.0},
    )
    raw = pd.DataFrame({
        "region": ["DEU", "DEU"],
        "technology": ["nuclear", "nuclear"],
        "parameter": ["investment", "lifetime"],
        "value": [100.0, 60.0],
        "unit": ["USD/MW", "yr"],
    })
    out = coupler.finalise_cost_parameters(raw).set_index("parameter")["value"]

    assert out["investment"] == pytest.approx(200.0)  # currency-denominated
    assert out["lifetime"] == pytest.approx(60.0)  # physical, untouched
    assert raw["value"].tolist() == [100.0, 60.0]  # caller's frame not mutated


def test_technology_mapping_example_matches_examples_dir():
    """tests/data's copy must stay byte-identical to the examples/ file it mirrors."""
    examples_copy = Path(__file__).parents[1] / "examples" / "technology-mapping.example.yaml"
    assert TECH_MAPPING.read_text() == examples_copy.read_text(), (
        f"{TECH_MAPPING} and {examples_copy} have diverged — update one to match the other."
    )


def _coupler(currency_factor: float = 1.0) -> RemindGdxCoupler:
    from iampypsa import IamLoader
    from iampypsa.quantities import load_quantity_specs

    loader = IamLoader(str(GDX))
    return RemindGdxCoupler(
        loader,
        load_quantity_specs(backend=loader.backend),
        region_map=REGION_MAP,
        config={
            "sector_weights": SECTOR_WEIGHTS,
            "countries": COUNTRIES,
            "planning_horizons": YEARS,
            "currency_factor": currency_factor,
        },
        model_regions=["DEU", "EWN", "CHA"],
        reference_data={
            "population": pd.read_csv(DATA / "ssp_population_filtered.csv").set_index(["iso2", "year"]),
            "gdp": pd.read_csv(DATA / "ssp_gdp_filtered.csv").set_index(["iso2", "year"]),
        },
    )


def test_build_co2_prices_matches_reference():
    got = _coupler().build_co2_prices(years=YEARS).set_index(["region", "year"])["value"]
    ref = pd.read_csv(DATA / "reference" / "co2_price.csv").set_index(["region", "year"])["co2_price"]
    pd.testing.assert_series_equal(got.sort_index(), ref.sort_index(), check_names=False, rtol=1e-9)


def test_build_country_loads_matches_reference():
    got = _coupler().downscale_country_demand().set_index(["year", "region", "sector"])["value"].sort_index()
    ref = (
        pd.read_csv(DATA / "reference" / "sectoral_load_country.csv")
        .set_index(["year", "region", "sector"])["value"]
        .sort_index()
    )
    assert got.index.equals(ref.index)
    pd.testing.assert_series_equal(got, ref, check_names=False, rtol=1e-9)


def test_cost_overrides_match_reference_remind_rows():
    """extract_cost_parameters (+ inline battery-inverter² as the EUR script does) vs the raw cost reference."""
    from iampypsa.quantities import load_technology_parameters
    from iampypsa.transforms.costs import (
        build_iam_techdata,
        convert_investment_to_input_capacity_basis,
    )

    remind_long = _coupler().extract_cost_parameters(2090)
    # The battery-inverter round-trip efficiency tweak is applied in import_REMIND_costs.py.
    is_eff = (remind_long["parameter"] == "efficiency") & (remind_long["technology"] == "battery-inverter")
    remind_long.loc[is_eff, "value"] **= 2

    technology_mapping = load_technology_parameters(str(TECH_MAPPING))["technologies"]
    overrides = convert_investment_to_input_capacity_basis(
        build_iam_techdata(technology_mapping, remind_long), ["electrolysis"]
    )
    got = (
        overrides.query("region == 'DEU'")
        .set_index(["technology", "parameter"])["value"]
        .sort_index()
    )
    ref = (
        pd.read_csv(DATA / "reference" / "costs_raw_overwritten.csv")
        .query("region == 'DEU' and source == 'IAM'")
        .set_index(["technology", "parameter"])["value"]
        .sort_index()
    )
    assert got.index.equals(ref.index)
    pd.testing.assert_series_equal(got, ref, check_names=False, rtol=1e-6)


def test_currency_factor_scales_gdx_cost_parameters():
    """currency_factor scales investment/VOM/fuel only, on the GDX path."""
    base = _coupler().extract_cost_parameters(2090).set_index(["region", "technology", "parameter"])["value"]
    scaled = (
        _coupler(currency_factor=0.9)
        .extract_cost_parameters(2090)
        .set_index(["region", "technology", "parameter"])["value"]
    )
    assert base.index.equals(scaled.index)

    currency_params = base.index.get_level_values("parameter").isin({"investment", "VOM", "fuel"})
    pd.testing.assert_series_equal(
        scaled[currency_params], base[currency_params] * 0.9, check_names=False, rtol=1e-9
    )
    pd.testing.assert_series_equal(
        scaled[~currency_params], base[~currency_params], check_names=False, rtol=1e-9
    )


def test_full_capacity_targets_match_reference():
    from iampypsa.models.remind import build_capacity_reporting_technologies
    from iampypsa.quantities import load_quantity_specs, load_technology_parameters
    from iampypsa.quantities.schema import get_iam_name

    technology_mapping = load_technology_parameters(str(TECH_MAPPING))["technologies"]
    reports_capacity = build_capacity_reporting_technologies(load_quantity_specs(backend="iamc"))
    tmap = pd.DataFrame(
        [
            {"PyPSA": tech, "IAM": get_iam_name(tech, spec)}
            for tech, spec in technology_mapping.items()
            if get_iam_name(tech, spec) in reports_capacity
        ]
    )
    a = _coupler()
    got = a.get_capacities(tmap, map_tech_col="IAM", map_carrier_col="PyPSA")
    got["year"] = got["year"].astype(int)
    g = got.query("region == 'DEU' and year == 2090").set_index("carrier")["value"]
    r = (
        pd.read_csv(DATA / "reference" / "installed_capacities.csv")
        .query("region == 'DEU' and year == 2090")
        .set_index("carrier")["value"]
    )
    assert g.index.equals(r.index)
    pd.testing.assert_series_equal(g, r, check_names=False, rtol=1e-6)


def test_prepare_capacities_stops_before_carrier_aggregation():
    """prepare_capacities is the model-tech seam consumers reach for (brownfield
    harmonisation); get_capacities is the same data aggregated to PyPSA carriers."""
    a = _coupler()
    raw = a.prepare_capacities()
    assert "technology" in raw.columns and "carrier" not in raw.columns

    tmap = pd.DataFrame(
        [{"PyPSA": "onwind", "IAM": "wind-onshore"}, {"PyPSA": "solar", "IAM": "solar-pv"}]
    )
    targets = a.get_capacities(tmap, map_tech_col="IAM", map_carrier_col="PyPSA")
    assert set(targets["carrier"]) == {"onwind", "solar"}
    assert set(targets["region"]) <= set(a.model_regions)
