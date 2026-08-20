"""Tests for the iampypsa.io loader (GDX + .mif backends, candidate resolution)."""

from pathlib import Path

import pytest

from iampypsa import IamLoader
from iampypsa.formats.iamc import read_iamc
from iampypsa.units import parse_currency_year

GDX = Path(__file__).parent / "data" / "remind2pypsa_amt_filtered.gdx"


def test_detect_backend():
    """The suffix→backend mapping lives in the formats registry, not in the loader."""
    from iampypsa.formats import detect_backend

    assert detect_backend("x.gdx") == "gdx"
    assert detect_backend("x.mif") == "iamc"
    assert detect_backend("x.csv") == "iamc"
    assert IamLoader("x.gdx").backend == "gdx"
    with pytest.raises(ValueError):
        detect_backend("x.nc")


def test_iamc_read_and_melt(tmp_path):
    # Round-trip: write a minimal .mif and check that read_iamc melts it correctly.
    mif = tmp_path / "remind.mif"
    mif.write_text(
        "Model;Scenario;Region;Variable;Unit;2025;2030\n"
        "REMIND;SSP2;DEU;Cap|Electricity|Gas|GT;GW;1.2;2.4\n"
        "REMIND;SSP2;DEU;Cap|Electricity|Nuclear;GW;NA;10.0\n"
    )
    assert IamLoader(mif).backend == "iamc"
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


def test_gdx_candidate_resolution_and_load():
    loader = IamLoader(str(GDX))
    assert loader.backend == "gdx"
    # first candidate absent (run/version rename), falls back to the present name
    assert loader.resolve(["v32_taxCO2eq", "p_priceCO2"]) == "p_priceCO2"
    with pytest.raises(KeyError):
        loader.resolve(["does_not_exist", "nor_this"])
    df = loader.read("p_priceCO2", rename_columns={"tall": "year", "all_regi": "region"})
    assert {"year", "region", "value"} <= set(df.columns)
    assert "DEU" in set(df["region"])
