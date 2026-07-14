"""Tests for load conversion and region→country downscaling (synthetic + real AMT data)."""

from pathlib import Path

import pandas as pd
import pytest

from iampypsa.downscale import (
    ProportionalDownscaler,
    build_ssp_shares,
    disaggregate_demand_to_country,
)
from iampypsa.transforms.loads import TWA_TO_MWH, convert_loads

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


def test_convert_loads_unit_and_grouping():
    raw = pd.DataFrame(
        {"year": [2030, 2030], "region": ["DEU", "DEU"], "sector": ["AC", "AC"], "value": [1.0, 0.5]}
    )
    out = convert_loads(raw)
    assert out["value"].iloc[0] == pytest.approx(1.5 * TWA_TO_MWH)  # summed then converted
    assert out["unit"].iloc[0] == "MWh_el"


def test_build_ssp_shares_blend_and_normalise():
    pop = pd.DataFrame({"iso2": ["DE", "FR"], "year": [2030, 2030], "value": [80.0, 60.0]}).set_index(["iso2", "year"])
    gdp = pd.DataFrame({"iso2": ["DE", "FR"], "year": [2030, 2030], "value": [40.0, 10.0]}).set_index(["iso2", "year"])
    shares = build_ssp_shares(["DE", "FR"], 2030, "AC", pop, gdp, {"AC": {"gdp": 0.5, "population": 0.5}})
    assert sum(shares.values()) == pytest.approx(1.0)
    # DE has higher GDP and pop -> larger share
    assert shares["DE"] > shares["FR"]


def test_proportional_downscaler_splits_by_share():
    coarse = pd.DataFrame({"region": ["EUR"], "sector": ["AC"], "value": [100.0]})
    shares = pd.DataFrame({"region": ["EUR", "EUR"], "fine_id": ["DE", "FR"], "share": [0.7, 0.3]})
    out = ProportionalDownscaler().downscale(coarse, shares).set_index("region")["value"]
    assert out["DE"] == pytest.approx(70.0)
    assert out["FR"] == pytest.approx(30.0)


def test_convert_loads_matches_reference_regional():
    from iampypsa.io import read_gdx_symbol as read_gdx

    raw = read_gdx(str(GDX), "p32_load_sector",
                   rename_columns={"ttot": "year", "all_regi": "region", "loadPy32": "sector"})
    raw["year"] = raw["year"].astype(int)
    got = convert_loads(raw, regions=["DEU"]).query(
        "region == 'DEU' and year == 2090 and sector == 'AC'"
    )["value"].iloc[0]
    ref = pd.read_csv(SECT_LOAD).query("region=='DEU' and year==2090 and sector=='AC'")["value"].iloc[0]
    assert got == pytest.approx(ref, rel=1e-9)


def test_full_ssp_downscaling_matches_reference():
    from iampypsa.couplers.remind import read_region_map

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
