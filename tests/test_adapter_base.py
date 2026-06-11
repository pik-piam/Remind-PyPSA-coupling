"""Composition test for CouplingAdapter: generic builders vs dev references."""

from __future__ import annotations

import os

import pandas as pd
import pytest

from rpycpl.adapters.base import CouplingAdapter

DEV = "/workspace/remind_pypsa_coupling/development_data/PkBudg1000_Europe_without_NES_fixed/i1"
GDX = f"{DEV}/REMIND2PyPSAEUR.gdx"
SSP = "/workspace/remind_pypsa_coupling/development_data/ssp"
REGION_MAP = "/workspace/pypsa-eur-aod/pypsa-eur/config/regionmapping_21_EU11.csv"
COST_MAP = "/workspace/pypsa-eur-aod/pypsa-eur/config/technology_cost_mapping.csv"
HAVE_DATA = all(os.path.exists(p) for p in [GDX, f"{SSP}/population.csv", REGION_MAP, COST_MAP])


class _MinimalAdapter(CouplingAdapter):
    """Concrete adapter implementing just the abstract hooks for the generic-builder test."""

    def build_config_overrides(self):
        return {}

    def extract_cost_parameters(self, year):
        return pd.DataFrame(columns=["region", "reference", "parameter", "value", "unit"])


def test_abstract_methods_enforced():
    with pytest.raises(TypeError):
        CouplingAdapter(None, {}, {}, {})  # cannot instantiate ABC


def _adapter():
    import yaml

    from rpycpl.io import RemindLoader
    from rpycpl.io.remind_symbols import load_symbol_specs
    from rpycpl.transforms.mapping import read_region_map

    cfg = yaml.safe_load(open(f"{DEV}/config.remind_europe_without_NES_fixed.yaml"))
    co2 = pd.read_csv(f"{DEV}/co2_price.csv")
    return _MinimalAdapter(
        loader=RemindLoader(GDX),
        symbols=load_symbol_specs(),
        region_map=read_region_map(REGION_MAP, source="REMIND-EU", target="PyPSA-EUR"),
        config={
            "sector_weights": cfg["remind_coupling"]["demand_downscaling"]["sector_weights"],
            "countries": cfg["countries"],
            "planning_horizons": sorted(co2["year"].unique()),
        },
        remind_regions=sorted(pd.read_csv(f"{DEV}/sectoral_load.csv")["region"].unique()),
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
def test_build_capacity_targets_generators_match_reference():
    mapping = pd.read_csv(COST_MAP).query("parameter == 'investment' and source == 'REMIND'")
    tmap = mapping[["PyPSA-Eur technology", "reference"]].rename(
        columns={"PyPSA-Eur technology": "PyPSA-Eur", "reference": "REMIND-EU"})
    got = _adapter().determine_must_build_capacity(tmap)
    got["year"] = got["year"].astype(int)
    ref = pd.read_csv(f"{DEV}/installed_capacities.csv").rename(columns={"region_REMIND": "region"})
    gens = ["ccgt", "ocgt", "onwind", "offwind", "solar", "nuclear"]
    g = got[got["carrier"].isin(gens)].set_index(["year", "region", "carrier"])["p_nom_min"]
    r = ref[ref["carrier"].isin(gens)].set_index(["year", "region", "carrier"])["p_nom_min"]
    shared = g.index.intersection(r.index)
    assert len(shared) > 100
    pd.testing.assert_series_equal(g.reindex(shared), r.reindex(shared), check_names=False, rtol=1e-6)
