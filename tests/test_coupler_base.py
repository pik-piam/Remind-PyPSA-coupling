"""Tests for the (directly instantiable) Coupler: builders vs reference CSVs.

The data-driven checks run against the filtered GDX fixture and reference CSVs in tests/data/
(self-contained -- see tests/data/README.md for provenance; no external '/workspace/...' paths).
"""

from pathlib import Path

import pandas as pd
import pytest

from iampypsa.couplers.base import Coupler
from iampypsa.couplers.remind import RemindGdxCoupler
from iampypsa.transforms.capacities import build_capacity_targets

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
    adapter = Coupler(loader=None, symbols={}, region_map={}, config={})
    assert isinstance(adapter, Coupler)


def test_technology_mapping_example_matches_examples_dir():
    """tests/data's copy must stay byte-identical to the examples/ file it mirrors."""
    examples_copy = Path(__file__).parents[1] / "examples" / "technology-mapping.example.yaml"
    assert TECH_MAPPING.read_text() == examples_copy.read_text(), (
        f"{TECH_MAPPING} and {examples_copy} have diverged — update one to match the other."
    )


def _coupler(currency_factor: float = 1.0) -> RemindGdxCoupler:
    from iampypsa.io import RemindLoader
    from iampypsa.io.remind_symbols import load_symbol_specs

    loader = RemindLoader(str(GDX))
    return RemindGdxCoupler(
        loader,
        load_symbol_specs(backend=loader.backend),
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
    from iampypsa.io import load_technology_parameters
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
        build_iam_techdata(technology_mapping, remind_long)
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
    from iampypsa.io import build_capacity_reporting_technologies, load_technology_parameters
    from iampypsa.io.technology_mapping import iam_name

    technology_mapping = load_technology_parameters(str(TECH_MAPPING))["technologies"]
    reports_capacity = build_capacity_reporting_technologies()
    tmap = pd.DataFrame(
        [
            {"PyPSA": tech, "IAM": iam_name(tech, spec)}
            for tech, spec in technology_mapping.items()
            if iam_name(tech, spec) in reports_capacity
        ]
    )
    a = _coupler()
    got = build_capacity_targets(
        a.loader, a.symbols, a.model_regions, tmap,
        map_tech_col="IAM", map_carrier_col="PyPSA",
    )
    got["year"] = got["year"].astype(int)
    g = got.query("region == 'DEU' and year == 2090").set_index("carrier")["value"]
    r = (
        pd.read_csv(DATA / "reference" / "installed_capacities.csv")
        .query("region == 'DEU' and year == 2090")
        .set_index("carrier")["value"]
    )
    assert g.index.equals(r.index)
    pd.testing.assert_series_equal(g, r, check_names=False, rtol=1e-6)
