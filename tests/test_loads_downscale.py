"""Tests for load conversion and region→country downscaling (synthetic + real AMT data)."""

from pathlib import Path

import pandas as pd
import pytest

from iampypsa.downscale import disaggregate_demand_to_country

DATA = Path(__file__).parent / "data"
GDX = DATA / "remind2pypsa_amt_filtered.gdx"
SECT_LOAD = DATA / "reference" / "sectoral_load.csv"
SECT_LOAD_COUNTRY = DATA / "reference" / "sectoral_load_country.csv"
REGION_MAP = DATA / "region_mapping_filtered.csv"
SECTOR_WEIGHTS = {
    "AC": {"gdp": 0.6, "population": 0.4},
    "electrolysis": {"gdp": 0.7, "population": 0.3},
    "EV_pass": {"gdp": 0.3, "population": 0.7},
    "EV_freight": {"gdp": 0.5, "population": 0.5},
    "heatpump": {"gdp": 0.3, "population": 0.7},
}
COUNTRIES = {"DE", "AT", "BE", "LU", "NL", "CN", "HK", "MO", "TW"}


def test_build_regional_demand_labels_and_groups():
    from iampypsa.models.remind import RemindGdxCoupler

    class _FakeLoader:
        backend = "gdx"

        def resolve(self, ref):
            return ref if isinstance(ref, str) else ref[0]

        def read(self, ref, rename_columns=None):
            return pd.DataFrame(
                {"year": [2030, 2030], "region": ["DEU", "DEU"], "sector": ["AC", "AC"], "value": [1.0, 0.5]}
            )

    quantities = {"demand_fe_sectors": {"symbol": "p32_load_sector"}}
    coupler = RemindGdxCoupler(_FakeLoader(), quantities, region_map={}, config={}, model_regions=["DEU"])
    out = coupler.build_regional_demand()
    assert out["value"].iloc[0] == pytest.approx(1.5)
    assert out["unit"].iloc[0] == "MWh_el"


def test_disaggregation_splits_proportionally_to_the_proxy():
    """A region's value is split by its members' proxy shares — 70/30 here, and it sums back."""
    coarse = pd.DataFrame(
        {"year": [2030], "region": ["EUR"], "sector": ["AC"], "value": [100.0], "unit": ["MWh"]}
    )
    gdp = pd.DataFrame(
        {"iso2": ["DE", "FR"], "year": [2030, 2030], "value": [70.0, 30.0]}
    ).set_index(["iso2", "year"])
    out = disaggregate_demand_to_country(
        coarse, {"EUR": ["DE", "FR"]}, {"gdp": gdp}, {"AC": {"gdp": 1.0}}, {"DE", "FR"}
    ).set_index("region")["value"]
    assert out["DE"] == pytest.approx(70.0)
    assert out["FR"] == pytest.approx(30.0)


def test_build_regional_demand_matches_reference_regional():
    from iampypsa.models.remind import RemindGdxCoupler
    from iampypsa import IamLoader
    from iampypsa.quantities import load_quantity_specs

    loader = IamLoader(str(GDX))
    quantities = load_quantity_specs(backend=loader.backend)
    coupler = RemindGdxCoupler(loader, quantities, region_map={}, config={}, model_regions=["DEU"])
    got = coupler.build_regional_demand().query(
        "region == 'DEU' and year == 2090 and sector == 'AC'"
    )["value"].iloc[0]
    ref = pd.read_csv(SECT_LOAD).query("region=='DEU' and year==2090 and sector=='AC'")["value"].iloc[0]
    assert got == pytest.approx(ref, rel=1e-9)


def test_full_ssp_downscaling_matches_reference():
    from iampypsa.models.remind import read_region_map

    region_to_countries = read_region_map(source="model_region", target="country", file_path=str(REGION_MAP))
    pop = pd.read_csv(DATA / "ssp_population_filtered.csv").set_index(["iso2", "year"])
    gdp = pd.read_csv(DATA / "ssp_gdp_filtered.csv").set_index(["iso2", "year"])

    ref = pd.read_csv(SECT_LOAD_COUNTRY)
    load_in = pd.read_csv(SECT_LOAD).query("year in @ref.year.unique()")
    got = disaggregate_demand_to_country(
        load_in, region_to_countries, {"population": pop, "gdp": gdp}, SECTOR_WEIGHTS, COUNTRIES
    )
    g = got.set_index(["year", "region", "sector"])["value"].sort_index()
    r = ref.set_index(["year", "region", "sector"])["value"].sort_index()
    assert g.index.equals(r.index)  # no missing/extra country-year-sector rows
    pd.testing.assert_series_equal(g, r, check_names=False, rtol=1e-9)


def test_disaggregate_single_country_is_noop_vs_reference():
    # DEU -> DE is single-member: disaggregation must copy values unchanged.
    sectoral_load = pd.read_csv(SECT_LOAD).query("region == 'DEU' and year == 2090")
    out = disaggregate_demand_to_country(
        sectoral_load,
        region_to_countries={"DEU": ["DE"]},
        proxies={},  # single-member region is a no-op; no proxy needed
        sector_weights={},
        configured_countries={"DE"},
    ).set_index("sector")["value"].sort_index()

    ref = (
        pd.read_csv(SECT_LOAD_COUNTRY)
        .query("region == 'DE' and year == 2090")
        .set_index("sector")["value"]
        .sort_index()
    )
    pd.testing.assert_series_equal(out, ref, check_names=False)
