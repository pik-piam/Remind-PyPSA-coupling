"""Tests for the central REMIND symbol config (no hardcoded names)."""

import pandas as pd
import pytest

from iampypsa.io.remind_symbols import (
    load_frame,
    load_set,
    load_spec,
    load_symbol_specs,
    load_variable_set,
    read_symbol_config,
    report_fallbacks,
)

from pathlib import Path

EUR_GDX = Path(__file__).parent / "data" / "remind2pypsa_amt_filtered.gdx"


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
    assert default["co2_price"]["symbol"] == "p_priceCO2"
    assert default["demand_fe_sectors"]["rename"]["loadPy32"] == "sector"


def test_merge_region_overrides_prefers_region_entry():
    """Pure dict-merge logic: a region entry overrides its logical name, others pass through."""
    from iampypsa.io.remind_symbols import merge_region_overrides

    config = {
        "default": {"co2_price": {"symbol": ["default_symbol"]}, "coupled_years": {"symbol": "t"}},
        "overrides": {"XYZ": {"co2_price": {"symbol": ["region_symbol"]}}},
    }
    merged = merge_region_overrides(config, "XYZ")
    assert merged["co2_price"]["symbol"] == ["region_symbol"]
    assert merged["coupled_years"] == config["default"]["coupled_years"]
    # unknown region falls back to default unchanged
    assert merge_region_overrides(config, "UNKNOWN") == config["default"]


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
    assert "demand_fe_sectors" in specs  # an entry only in the package default is still present
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
    assert default_symbol_config_path(backend="iamc").name == "remind_symbols_mif.yaml"
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
    assert out["unit"].iloc[0] == "$/tCO2"  # stamped from to_unit


def test_load_frame_no_conversion_without_to_unit():
    loader = _FakeLoader({"sym": pd.DataFrame({"value": [5.0]})})
    out = load_frame(loader, {"symbol": "sym", "unit": "T$/TWa"})  # no to_unit → untouched
    assert out["value"].iloc[0] == 5.0
    assert out["unit"].iloc[0] == "T$/TWa"  # stamped from the declared source unit


def test_load_frame_no_unit_column_when_unit_undeclared():
    loader = _FakeLoader({"sym": pd.DataFrame({"value": [5.0]})})
    out = load_frame(loader, {"symbol": "sym"})  # no unit/to_unit declared at all
    assert "unit" not in out.columns


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


def test_load_spec_variables_shape_rejects_gdx_backend():
    # A `variables:` spec dispatches to load_variable_set regardless of backend, which
    # raises clearly if the loader isn't IAMC-backed — spec-shape dispatch doesn't imply
    # every shape is satisfiable by every backend.
    loader = _FakeLoader({})
    loader.backend = "gdx"
    var_set_spec = {"variables": {"Cap|Electricity|Gas|GT": "gas-ocgt"}, "label_col": "technology", "to_unit": "MW"}
    with pytest.raises(ValueError, match="requires an IAMC-backed loader"):
        load_spec(loader, var_set_spec)


def test_report_fallbacks_lists_all():
    iamc = load_symbol_specs(backend="iamc")
    fb = report_fallbacks(iamc)
    assert set(fb.columns) == {"logical_name", "token", "value", "reason"}
    # nuclear efficiency is no longer a declared fallback — it's computed directly from the
    # mif's uranium mass-basis price/conversion-factor variables (see RemindIamcCoupler).
    efficiency_fb = fb[fb["logical_name"] == "efficiency"]
    assert "nuclear" not in set(efficiency_fb["token"])
    # CO2 intensity fallbacks: biomass techs (carbon-neutral) plus zero-emission technologies
    # with no mif variable at all (no direct emissions) — all real values, not data gaps.
    emission_fb = fb[fb["logical_name"] == "emission_factor"]
    assert set(emission_fb["token"]) == {
        "biomass-chp", "biomass-igcc", "solar-pv", "wind-onshore", "wind-offshore",
        "hydro", "nuclear", "electrolysis", "hydrogen-turbine",
    }
    assert (emission_fb["value"] == 0.0).all()


def _mif_canonical_names() -> set[str]:
    """Every canonical name used as a `variables:`/`derived:`/`fallback:` label in the mif config."""
    mif = read_symbol_config(backend="iamc")["default"]
    names: set[str] = set()
    for spec in mif.values():
        if not isinstance(spec, dict):
            continue
        names |= set(spec.get("variables", {}).values())
        names |= set(spec.get("derived", {}))
        names |= set(spec.get("fallback", {}))
    return names


def test_mif_vocabulary_matches_gdx_technology_names():
    """remind_symbols_mif.yaml's canonical names must be exactly the values gdx tokens map to."""
    gdx = read_symbol_config(backend="gdx")["default"]
    canonical_values = set(gdx["technology_names"].values())
    mif_names = _mif_canonical_names()
    # mif also carries demand-sector labels (EV_pass, heatpump, ...) which have no gdx-token
    # counterpart in technology_names (they're sector labels, not technology tokens).
    demand_sectors = {"EV_pass", "EV_freight", "heatpump", "resistive", "space_cooling"}
    assert mif_names - demand_sectors <= canonical_values
    assert (mif_names - demand_sectors) & canonical_values  # sanity: not vacuously true


def test_tech_fuel_map_is_keyed_by_the_canonical_vocabulary():
    """The mif tech_fuel_map is keyed by canonical names, not raw tokens. (The gdx has no static
    tech_fuel_map — it derives one from pe2se at runtime; see RemindGdxCoupler._tech_fuel_map_from_pe2se.)"""
    gdx = read_symbol_config(backend="gdx")["default"]
    mif = read_symbol_config(backend="iamc")["default"]
    assert "tech_fuel_map" not in gdx  # gdx derives it from pe2se instead
    canonical_values = set(gdx["technology_names"].values())
    tfm = mif["tech_fuel_map"]
    assert set(tfm) <= canonical_values
    assert set(tfm.values()) <= canonical_values


def test_load_frame_against_real_gdx():
    from iampypsa.io import RemindLoader

    loader = RemindLoader(str(EUR_GDX))
    spec = load_symbol_specs()["co2_price"]
    df = load_frame(loader, spec)
    assert {"year", "region", "value"} <= set(df.columns)
    assert "DEU" in set(df["region"])
