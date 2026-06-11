"""Tests for the rpycpl.io loader (GDX + .mif backends, candidate resolution)."""

from __future__ import annotations

import os

import pytest

from rpycpl.io import RemindLoader, read_iamc

EUR_GDX = "/workspace/remind_pypsa_coupling/development_data/REMIND2PyPSAEUR.gdx"


def test_detect_backend():
    assert RemindLoader.detect_backend("x.gdx") == "gdx"
    assert RemindLoader.detect_backend("x.mif") == "iamc"
    assert RemindLoader.detect_backend("x.csv") == "iamc"
    with pytest.raises(ValueError):
        RemindLoader.detect_backend("x.nc")


def test_iamc_reader_is_shell(tmp_path):
    # IAMC/.mif reader is a shell for now — backend resolves but reading is not implemented.
    p = tmp_path / "remind.mif"
    p.write_text("placeholder")
    assert RemindLoader(p).backend == "iamc"
    with pytest.raises(NotImplementedError):
        read_iamc(p)


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
