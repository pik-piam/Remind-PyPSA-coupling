"""Tests for load conversion and region→country downscaling (synthetic + real EUR data)."""

from __future__ import annotations

import os

import pandas as pd
import pytest

from rpycpl.downscale import (
    ProportionalDownscaler,
    build_ssp_shares,
    disaggregate_demand_to_country,
)
from rpycpl.transforms.loads import TWA_TO_MWH, convert_loads

DEV = "/workspace/remind_pypsa_coupling/development_data/PkBudg1000_Europe_without_NES_fixed/i1"
EUR_GDX = f"{DEV}/REMIND2PyPSAEUR.gdx"
SECT_LOAD = f"{DEV}/sectoral_load.csv"
SECT_LOAD_COUNTRY = f"{DEV}/sectoral_load_country.csv"


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


@pytest.mark.skipif(not os.path.exists(EUR_GDX), reason="EUR development GDX not present")
def test_convert_loads_matches_reference_regional():
    from rpycpl.io import read_gdx_symbol as read_gdx

    raw = read_gdx(EUR_GDX, "p32_load_sector",
                   rename_columns={"ttot": "year", "all_regi": "region", "loadPy32": "sector"})
    raw["year"] = raw["year"].astype(int)
    got = convert_loads(raw, regions=["DEU"]).query(
        "region == 'DEU' and year == 2030 and sector == 'AC'"
    )["value"].iloc[0]
    ref = pd.read_csv(SECT_LOAD).query("region=='DEU' and year==2030 and sector=='AC'")["value"].iloc[0]
    assert got == pytest.approx(ref, rel=1e-9)


SSP_DIR = "/workspace/remind_pypsa_coupling/development_data/ssp"
REGION_MAP = "/workspace/pypsa-eur-aod/pypsa-eur/config/regionmapping_21_EU11.csv"
CONFIG = (
    "/workspace/remind_pypsa_coupling/development_data/PkBudg1000_Europe_without_NES_fixed/i1/"
    "config.remind_europe_without_NES_fixed.yaml"
)


@pytest.mark.skipif(
    not (os.path.exists(f"{SSP_DIR}/population.csv") and os.path.exists(REGION_MAP)
         and os.path.exists(SECT_LOAD_COUNTRY) and os.path.exists(CONFIG)),
    reason="SSP/region-map/reference data not present",
)
def test_full_ssp_downscaling_matches_reference():
    import yaml

    from rpycpl.transforms.mapping import read_region_map

    cfg = yaml.safe_load(open(CONFIG))
    sector_weights = cfg["remind_coupling"]["demand_downscaling"]["sector_weights"]
    configured = set(cfg["countries"])
    region_to_countries = read_region_map(REGION_MAP, source="REMIND-EU", target="PyPSA-EUR")
    pop = pd.read_csv(f"{SSP_DIR}/population.csv").set_index(["iso2", "year"])
    gdp = pd.read_csv(f"{SSP_DIR}/gdp.csv").set_index(["iso2", "year"])

    ref = pd.read_csv(SECT_LOAD_COUNTRY)
    load_in = pd.read_csv(SECT_LOAD).query("year in @ref.year.unique()")
    got = disaggregate_demand_to_country(
        load_in, region_to_countries, {"population": pop, "gdp": gdp}, sector_weights, configured
    )
    g = got.set_index(["year", "region", "sector"])["value"].sort_index()
    r = ref.set_index(["year", "region", "sector"])["value"].sort_index()
    assert g.index.equals(r.index)  # no missing/extra country-year-sector rows
    pd.testing.assert_series_equal(g, r, check_names=False, rtol=1e-9)


@pytest.mark.skipif(not os.path.exists(SECT_LOAD_COUNTRY), reason="reference not present")
def test_disaggregate_single_country_is_noop_vs_reference():
    # DEU -> DE is single-member: disaggregation must copy values unchanged.
    sectoral_load = pd.read_csv(SECT_LOAD).query("region == 'DEU' and year == 2030")
    out = disaggregate_demand_to_country(
        sectoral_load,
        region_to_countries={"DEU": ["DE"]},
        proxies={},  # single-member region is a no-op; no proxy needed
        sector_weights={},
        configured_countries={"DE"},
    ).set_index("sector")["value"].sort_index()

    ref = (
        pd.read_csv(SECT_LOAD_COUNTRY)
        .query("region == 'DE' and year == 2030")
        .set_index("sector")["value"]
        .sort_index()
    )
    pd.testing.assert_series_equal(out, ref, check_names=False)
