"""Tests for the central REMIND symbol config (no hardcoded names)."""

from __future__ import annotations

import os

import pandas as pd
import pytest

from iampypsa.io.remind_symbols import (
    load_frame,
    load_set,
    load_spec,
    load_symbol_specs,
    load_variable_set,
    report_fallbacks,
)

EUR_GDX = "/workspace/remind_pypsa_coupling/development_data/REMIND2PyPSAEUR.gdx"


class _FakeLoader:
    """Minimal loader: resolves the first candidate present in ``frames`` and returns it."""

    def __init__(self, frames: dict[str, pd.DataFrame]):
        self.frames = frames

    def resolve_symbol(self, ref):
        candidates = [ref] if isinstance(ref, str) else list(ref)
        for name in candidates:
            if name in self.frames:
                return name
        raise KeyError(ref)

    def load_symbol(self, ref, rename_columns=None):
        return self.frames[self.resolve_symbol(ref)].copy()


def test_load_specs_default():
    default = load_symbol_specs()
    assert default["co2_price"]["symbol"] == ["v32_taxCO2eq", "p_priceCO2"]  # candidate fallback
    assert default["load_sector"]["rename"]["loadPy32"] == "sector"


def test_load_specs_region_override_merges():
    default = load_symbol_specs()
    cha = load_symbol_specs("CHA")
    # overridden entry differs from default
    assert cha["co2_price"]["symbol"] == ["pm_taxCO2eq"]
    # non-overridden entry still equals default (CHA does not override coupled_years)
    assert cha["coupled_years"] == default["coupled_years"]


def test_load_specs_region_without_override_equals_default():
    assert load_symbol_specs("DEU") == load_symbol_specs()


OVERLAY = (
    "default:\n"
    "  co2_price:\n"
    "    symbol: my_symbol\n"
    "    rename: {a: year}\n"
    "overrides:\n"
    "  CHA:\n"
    "    co2_price:\n"
    "      symbol: cha_symbol\n"
)


def test_overlay_path_layers_onto_package_default(tmp_path):
    p = tmp_path / "syms.yaml"
    p.write_text(OVERLAY)
    specs = load_symbol_specs(path=p)
    assert specs["co2_price"]["symbol"] == "my_symbol"  # overlay overrides the package default
    assert "load_sector" in specs  # an entry only in the package default is still present
    cha = load_symbol_specs("CHA", path=p)
    assert cha["co2_price"]["symbol"] == "cha_symbol"  # overlay region override wins


def test_overlay_via_env_var(tmp_path, monkeypatch):
    from iampypsa.io.remind_symbols import SYMBOL_CONFIG_ENV

    p = tmp_path / "syms.yaml"
    p.write_text(OVERLAY)
    monkeypatch.setenv(SYMBOL_CONFIG_ENV, str(p))
    assert load_symbol_specs()["co2_price"]["symbol"] == "my_symbol"


def test_default_symbol_config_path_exists():
    from iampypsa.io.remind_symbols import default_symbol_config_path

    # Default (no backend) and explicit "gdx" both resolve to the GDX config.
    assert default_symbol_config_path().name == "remind_symbols_gdx.yaml"
    assert default_symbol_config_path(backend="gdx").name == "remind_symbols_gdx.yaml"
    assert default_symbol_config_path(backend="iamc").name == "remind_symbols_iamc.yaml"
    assert "default:" in default_symbol_config_path().read_text()


def test_load_frame_applies_per_candidate_unit():
    # Primary missing → resolves the fallback whose unit ($/tC) triggers the 12/44 conversion.
    loader = _FakeLoader({"p_priceCO2": pd.DataFrame({"region": ["DEU"], "value": [100.0]})})
    spec = {
        "symbol": ["v32_taxCO2eq", "p_priceCO2"],
        "units": ["$/tC", "$/tC"],
        "to_unit": "$/tCO2",
    }
    out = load_frame(loader, spec)
    assert out["value"].iloc[0] == pytest.approx(100.0 * 12 / 44)


def test_load_frame_no_conversion_without_to_unit():
    loader = _FakeLoader({"sym": pd.DataFrame({"value": [5.0]})})
    out = load_frame(loader, {"symbol": "sym", "unit": "T$/TWa"})  # no to_unit → untouched
    assert out["value"].iloc[0] == 5.0


def test_load_set_splits_mixed_units_via_schema():
    raw = pd.DataFrame(
        {
            "technology": ["ngcc", "ngcc", "ngcc"],
            "char": ["lifetime", "omf", "omv"],
            "value": [30.0, 0.05, 2.0],
        }
    )
    loader = _FakeLoader({"pm_data": raw})
    spec = {
        "symbol": "pm_data",
        "index": "char",
        "schema": {
            "lifetime": {"parameter": "lifetime", "unit": "yr", "to_unit": "yr"},
            "omf": {"parameter": "FOM", "unit": "p.u.", "to_unit": "%/yr"},
            "omv": {"parameter": "VOM", "unit": "T$/TWa", "to_unit": "$/MWh"},
        },
    }
    out = load_set(loader, spec).set_index("parameter")
    assert out.loc["lifetime", "value"] == 30.0          # factor 1
    assert out.loc["FOM", "value"] == pytest.approx(5.0)  # 0.05 * 100
    assert out.loc["VOM", "value"] == pytest.approx(2.0 * 1e6 / 8760)
    assert out.loc["VOM", "unit"] == "$/MWh"


def test_load_symbol_specs_iamc_backend():
    iamc = load_symbol_specs(backend="iamc")
    # IAMC config has no GDX symbols but has capacity variable-set
    assert "variables" in iamc["capacity"]
    assert "Cap|Electricity|Gas|GT" in iamc["capacity"]["variables"]
    # Discount rate is a single symbol (not a variable-set)
    assert iamc["discount_rate"]["symbol"] == "Interest Rate t/(t-1)|Real"
    # GDX backend still has symbol: (not variables:)
    gdx = load_symbol_specs(backend="gdx")
    assert "symbol" in gdx["capacity"]


def test_load_variable_set_basic(tmp_path):
    from iampypsa.io import RemindLoader
    from iampypsa.io.iamc import read_iamc

    mif = tmp_path / "t.mif"
    mif.write_text(
        "Model;Scenario;Region;Variable;Unit;2030;2040\n"
        "REMIND;SSP2;DEU;Cap|Electricity|Gas|GT;GW;1.0;2.0\n"
        "REMIND;SSP2;DEU;Cap|Electricity|Nuclear;GW;5.0;5.0\n"
        "REMIND;SSP2;DEU;Cap|Electricity|Coal|w/o CC;GW;10.0;8.0\n"
        "REMIND;SSP2;DEU;Cap|Electricity|Coal|IGCC|w/o CC;GW;3.0;2.0\n"
        "REMIND;SSP2;DEU;Cap|Electricity|Coal|CHP;GW;2.0;2.0\n"
    )
    loader = RemindLoader(mif)
    spec = {
        "variables": {
            "Cap|Electricity|Gas|GT": "ngt",
            "Cap|Electricity|Nuclear": "tnrs",
        },
        "derived": {
            "pc": [
                (1.0, "Cap|Electricity|Coal|w/o CC"),
                (-1.0, "Cap|Electricity|Coal|IGCC|w/o CC"),
                (-1.0, "Cap|Electricity|Coal|CHP"),
            ],
        },
        "label_col": "technology",
        "to_unit": "MW",
    }
    result = load_variable_set(loader, spec)
    assert set(result.columns) >= {"year", "region", "technology", "value", "unit"}
    pc_2030 = result[(result["technology"] == "pc") & (result["year"] == 2030)]["value"].iloc[0]
    assert pc_2030 == pytest.approx(5000.0)  # (10-3-2) GW × 1000 = 5000 MW
    assert all(result["unit"] == "MW")


def test_load_variable_set_synthesises_fallback_rows(tmp_path):
    """Absent tokens with a declared fallback value are appended across the (year, region) grid."""
    from iampypsa.io import RemindLoader

    mif = tmp_path / "t.mif"
    mif.write_text(
        "Model;Scenario;Region;Variable;Unit;2030;2040\n"
        "REMIND;SSP2;DEU;Efficiency|Electricity|Gas|GT;p.u.;0.58;0.60\n"
        "REMIND;SSP2;FRA;Efficiency|Electricity|Gas|GT;p.u.;0.57;0.59\n"
    )
    loader = RemindLoader(mif)
    spec = {
        "variables": {"Efficiency|Electricity|Gas|GT": "ngt"},
        "label_col": "reference",
        "to_unit": "p.u.",
        "fallback": {
            "tnrs": {"value": 0.33, "reason": "nuclear absent from mif"},
        },
    }
    result = load_variable_set(loader, spec)

    # ngt loaded normally
    assert set(result[result["reference"] == "ngt"]["year"].unique()) == {2030, 2040}
    # tnrs synthesised for every (year, region) combo present in the data
    tnrs = result[result["reference"] == "tnrs"]
    assert set(tnrs["year"].unique()) == {2030, 2040}
    assert set(tnrs["region"].unique()) == {"DEU", "FRA"}
    assert (tnrs["value"] == 0.33).all()
    assert (tnrs["unit"] == "p.u.").all()  # defaults to to_unit


def test_load_spec_dispatches_on_shape(tmp_path):
    from iampypsa.io import RemindLoader

    mif = tmp_path / "t.mif"
    mif.write_text(
        "Model;Scenario;Region;Variable;Unit;2030\n"
        "REMIND;SSP2;DEU;Cap|Electricity|Gas|GT;GW;1.5\n"
    )
    loader = RemindLoader(mif)
    var_set_spec = {"variables": {"Cap|Electricity|Gas|GT": "ngt"}, "label_col": "technology", "to_unit": "MW"}
    result = load_spec(loader, var_set_spec)
    assert len(result) == 1
    assert result["value"].iloc[0] == pytest.approx(1500.0)


def test_report_fallbacks_lists_all():
    iamc = load_symbol_specs(backend="iamc")
    fb = report_fallbacks(iamc)
    assert set(fb.columns) == {"logical_name", "token", "value", "reason"}
    # nuclear efficiency fallback is declared
    tnrs_row = fb[fb["token"] == "tnrs"]
    assert len(tnrs_row) == 1
    assert tnrs_row["value"].iloc[0] == pytest.approx(0.33)
    # CO2 intensity fallbacks (pecoal, pegas, peoil, pebiolc)
    assert set(fb["token"]).issuperset({"pecoal", "pegas", "peoil", "pebiolc"})


@pytest.mark.skipif(not os.path.exists(EUR_GDX), reason="EUR development GDX not present")
def test_load_frame_resolves_candidate_against_real_gdx():
    from iampypsa.io import RemindLoader

    loader = RemindLoader(EUR_GDX)
    spec = load_symbol_specs()["co2_price"]
    # v32_taxCO2eq is absent -> falls back to p_priceCO2; rename applied
    df = load_frame(loader, spec)
    assert {"year", "region", "value"} <= set(df.columns)
    assert "DEU" in set(df["region"])
