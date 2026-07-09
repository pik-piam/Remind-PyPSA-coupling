"""Tests for the iampypsa.io loader (GDX + .mif backends, candidate resolution)."""

from __future__ import annotations

import os

import pytest

from iampypsa.io import RemindLoader, read_iamc
from iampypsa.io.iamc import parse_currency_year

EUR_GDX = "/workspace/remind_pypsa_coupling/development_data/REMIND2PyPSAEUR.gdx"


def test_detect_backend():
    assert RemindLoader.detect_backend("x.gdx") == "gdx"
    assert RemindLoader.detect_backend("x.mif") == "iamc"
    assert RemindLoader.detect_backend("x.csv") == "iamc"
    with pytest.raises(ValueError):
        RemindLoader.detect_backend("x.nc")


def test_iamc_read_and_melt(tmp_path):
    # Round-trip: write a minimal .mif and check that read_iamc melts it correctly.
    mif = tmp_path / "remind.mif"
    mif.write_text(
        "Model;Scenario;Region;Variable;Unit;2025;2030\n"
        "REMIND;SSP2;DEU;Cap|Electricity|Gas|GT;GW;1.2;2.4\n"
        "REMIND;SSP2;DEU;Cap|Electricity|Nuclear;GW;NA;10.0\n"
    )
    assert RemindLoader(mif).backend == "iamc"
    df = read_iamc(mif)
    assert set(df.columns) >= {"model", "scenario", "region", "variable", "unit", "year", "value"}
    # NA rows are dropped; 2025 Nuclear is NaN → only 3 rows remain.
    assert len(df) == 3
    assert set(df["year"].unique()) == {2025, 2030}
    # Filter by variable
    filtered = read_iamc(mif, variables=["Cap|Electricity|Nuclear"])
    assert list(filtered["variable"].unique()) == ["Cap|Electricity|Nuclear"]
    assert len(filtered) == 1  # only the 2030 row (2025 was NA)


def test_parse_currency_year():
    assert parse_currency_year("US$2017/kW") == 2017
    assert parse_currency_year("US$2005/GJ") == 2005
    assert parse_currency_year("GW") is None
    assert parse_currency_year("EUR2020/MWh") is None


@pytest.mark.skipif(not os.path.exists(EUR_GDX), reason="EUR development GDX not present")
def test_gdx_candidate_resolution_and_load():
    loader = RemindLoader(EUR_GDX)
    assert loader.backend == "gdx"
    # first candidate absent (run/version rename), falls back to the present name
    assert loader.resolve_symbol(["v32_taxCO2eq", "p_priceCO2"]) == "p_priceCO2"
    with pytest.raises(KeyError):
        loader.resolve_symbol(["does_not_exist", "nor_this"])
    df = loader.load_symbol("p_priceCO2", rename_columns={"tall": "year", "all_regi": "region"})
    assert {"year", "region", "value"} <= set(df.columns)
    assert "DEU" in set(df["region"])
