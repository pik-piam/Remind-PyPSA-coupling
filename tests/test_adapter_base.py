"""Tests for the (directly instantiable) CouplingAdapter: builders vs dev references.

The adapter is now concrete — no subclass is required. The data-driven checks run against the
development GDX/reference CSVs when present (they depend only on iampypsa + pandas).
"""

from __future__ import annotations

import os

import pandas as pd
import pytest

from iampypsa.adapters.base import CouplingAdapter
from iampypsa.transforms.capacities import build_capacity_targets

DEV = "/workspace/remind_pypsa_coupling/development_data/PkBudg1000_Europe_without_NES_fixed/i1"
GDX = f"{DEV}/REMIND2PyPSAEUR.gdx"
SSP = "/workspace/remind_pypsa_coupling/development_data/ssp"
REGION_MAP = "/workspace/pypsa-eur-aod/pypsa-eur/config/regionmapping_21_EU11.csv"
COST_MAP = "/workspace/pypsa-eur-aod/pypsa-eur/config/technology_cost_mapping.csv"
HAVE_DATA = all(os.path.exists(p) for p in [GDX, f"{SSP}/population.csv", REGION_MAP, COST_MAP])


def test_adapter_is_directly_instantiable():
    """CouplingAdapter has no abstract methods — it can be created without a subclass."""
    adapter = CouplingAdapter(loader=None, symbols={}, region_map={}, config={})
    assert isinstance(adapter, CouplingAdapter)


def _adapter():
    import yaml

    from iampypsa.io import RemindLoader
    from iampypsa.io.remind_symbols import load_symbol_specs
    from iampypsa.transforms.mapping import read_region_map

    cfg = yaml.safe_load(open(f"{DEV}/config.remind_europe_without_NES_fixed.yaml"))
    co2 = pd.read_csv(f"{DEV}/co2_price.csv")
    return CouplingAdapter(
        loader=RemindLoader(GDX),
        symbols=load_symbol_specs(),
        region_map=read_region_map(REGION_MAP, source="REMIND-EU", target="PyPSA-EUR"),
        config={
            "sector_weights": cfg["remind_coupling"]["demand_downscaling"]["sector_weights"],
            "countries": cfg["countries"],
            "planning_horizons": sorted(co2["year"].unique()),
        },
        model_regions=sorted(pd.read_csv(f"{DEV}/sectoral_load.csv")["region"].unique()),
        ssp_population=pd.read_csv(f"{SSP}/population.csv").set_index(["iso2", "year"]),
        ssp_gdp=pd.read_csv(f"{SSP}/gdp.csv").set_index(["iso2", "year"]),
    )


@pytest.mark.skipif(not HAVE_DATA, reason="dev data not present")
def test_build_co2_prices_matches_reference():
    got = _adapter().build_co2_prices().set_index(["region", "year"])["value"]
    ref = pd.read_csv(f"{DEV}/co2_price.csv").set_index(["region", "year"])["co2_price"]
    shared = got.index.intersection(ref.index)
    assert len(shared) > 100
    pd.testing.assert_series_equal(got.reindex(shared), ref.reindex(shared), check_names=False, rtol=1e-9)


@pytest.mark.skipif(not HAVE_DATA, reason="dev data not present")
def test_build_country_loads_matches_reference():
    got = _adapter().downscale_country_demand().set_index(["year", "region", "sector"])["value"].sort_index()
    ref = pd.read_csv(f"{DEV}/sectoral_load_country.csv").set_index(["year", "region", "sector"])["value"].sort_index()
    assert got.index.equals(ref.index)
    pd.testing.assert_series_equal(got, ref, check_names=False, rtol=1e-9)


@pytest.mark.skipif(not HAVE_DATA, reason="dev data not present")
def test_cost_overrides_match_reference_remind_rows():
    """extract_cost_parameters (+ inline btin² as the EUR script does) vs the raw cost reference."""
    from iampypsa.transforms.costs import (
        build_iam_techdata,
        convert_investment_to_input_capacity_basis,
    )

    remind_long = _adapter().extract_cost_parameters(2050)
    # The btin (battery-inverter) round-trip efficiency tweak is applied in import_REMIND_costs.py.
    is_btin_eff = (remind_long["parameter"] == "efficiency") & (remind_long["reference"] == "btin")
    remind_long.loc[is_btin_eff, "value"] **= 2

    tech_map = pd.read_csv(COST_MAP)
    overrides = convert_investment_to_input_capacity_basis(
        build_iam_techdata(
            tech_map, remind_long,
            tech_col="PyPSA-Eur technology", ref_col="reference",
            param_col="parameter", source_col="source", model_value="REMIND", out_source="REMIND-EU",
        )
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


@pytest.mark.skipif(not HAVE_DATA, reason="dev data not present")
def test_full_capacity_targets_match_reference():
    mapping = pd.read_csv(COST_MAP).query("parameter == 'investment' and source == 'REMIND'")
    tmap = mapping[["PyPSA-Eur technology", "reference"]].rename(
        columns={"PyPSA-Eur technology": "PyPSA-Eur", "reference": "REMIND-EU"})
    a = _adapter()
    got = build_capacity_targets(a.loader, a.symbols, a.model_regions, tmap)
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
