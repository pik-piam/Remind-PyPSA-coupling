"""Validate the PyPSA-Eur adapter (cost extraction + capacity prep) vs dev references.

The adapter lives in the PyPSA-Eur repo; it is loaded here by path and exercised against the
development GDX/reference CSVs (it depends only on rpycpl + pandas).
"""

from __future__ import annotations

import importlib.util
import os

import pandas as pd
import pytest

ADAPTER = "/workspace/pypsa-eur-aod/pypsa-eur/scripts/remind/adapter_remind_eur.py"
DEV = "/workspace/remind_pypsa_coupling/development_data/PkBudg1000_Europe_without_NES_fixed/i1"
GDX = f"{DEV}/REMIND2PyPSAEUR.gdx"
SSP = "/workspace/remind_pypsa_coupling/development_data/ssp"
REGION_MAP = "/workspace/pypsa-eur-aod/pypsa-eur/config/regionmapping_21_EU11.csv"
COST_MAP = "/workspace/pypsa-eur-aod/pypsa-eur/config/technology_cost_mapping.csv"
HAVE = all(os.path.exists(p) for p in [ADAPTER, GDX, f"{SSP}/population.csv", REGION_MAP, COST_MAP])
pytestmark = pytest.mark.skipif(not HAVE, reason="EUR adapter or dev data not present")


def _load_adapter_cls():
    spec = importlib.util.spec_from_file_location("adapter_remind_eur", ADAPTER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.RemindEurAdapter, mod.LINK_TECHS


def _adapter():
    import yaml

    from rpycpl.io import RemindLoader
    from rpycpl.io.remind_symbols import load_symbol_specs
    from rpycpl.transforms.mapping import read_region_map

    cls, link_techs = _load_adapter_cls()
    cfg = yaml.safe_load(open(f"{DEV}/config.remind_europe_without_NES_fixed.yaml"))
    co2 = pd.read_csv(f"{DEV}/co2_price.csv")
    return cls(
        loader=RemindLoader(GDX),
        symbols=load_symbol_specs(),
        region_map=read_region_map(REGION_MAP, source="REMIND-EU", target="PyPSA-EUR"),
        config={
            "sector_weights": cfg["remind_coupling"]["demand_downscaling"]["sector_weights"],
            "countries": cfg["countries"],
            "planning_horizons": sorted(co2["year"].unique()),
            "link_techs": link_techs,
        },
        remind_regions=sorted(pd.read_csv(f"{DEV}/sectoral_load.csv")["region"].unique()),
        ssp_population=pd.read_csv(f"{SSP}/population.csv").set_index(["iso2", "year"]),
        ssp_gdp=pd.read_csv(f"{SSP}/gdp.csv").set_index(["iso2", "year"]),
    )


def test_cost_overrides_match_reference_remind_rows():
    from rpycpl.transforms.costs import (
        build_cost_overrides,
        convert_investment_to_input_capacity_basis,
    )

    remind_long = _adapter().extract_cost_parameters(2050)
    tech_map = pd.read_csv(COST_MAP)
    overrides = convert_investment_to_input_capacity_basis(
        build_cost_overrides(tech_map, remind_long)
    )
    got = (
        overrides.query("region == 'DEU'")
        .set_index(["technology", "parameter"])["value"]
        .sort_index()
    )
    ref = (
        pd.read_csv(f"{DEV}/y2050/costs_raw_overwritten.csv")
        .query("region == 'DEU' and source == 'REMIND-EU'")
        .set_index(["technology", "parameter"])["value"]
        .sort_index()
    )
    shared = got.index.intersection(ref.index)
    assert len(shared) > 10
    pd.testing.assert_series_equal(got.reindex(shared), ref.reindex(shared), check_names=False, rtol=1e-6)


def test_full_capacity_targets_match_reference():
    mapping = pd.read_csv(COST_MAP).query("parameter == 'investment' and source == 'REMIND'")
    tmap = mapping[["PyPSA-Eur technology", "reference"]].rename(
        columns={"PyPSA-Eur technology": "PyPSA-Eur", "reference": "REMIND-EU"})
    got = _adapter().determine_must_build_capacity(tmap)
    got["year"] = got["year"].astype(int)
    g = got.query("region == 'DEU' and year == 2050").set_index("carrier")["p_nom_min"]
    r = (
        pd.read_csv(f"{DEV}/installed_capacities.csv")
        .rename(columns={"region_REMIND": "region"})
        .query("region == 'DEU' and year == 2050")
        .set_index("carrier")["p_nom_min"]
    )
    shared = g.index.intersection(r.index)
    assert len(shared) >= 15  # generators + battery inverter + electrolysis + fuel cell + ...
    pd.testing.assert_series_equal(g.reindex(shared), r.reindex(shared), check_names=False, rtol=1e-6)
