"""Tests for shared cost-override mechanics (synthetic + real EUR GDX/reference)."""

from pathlib import Path

import pandas as pd
import pytest

from iampypsa.transforms.costs import (
    add_discount_rate,
    broadcast_fuel_prices,
    build_pypsa_techdata,
    build_iam_techdata,
    convert_investment_to_input_capacity_basis,
    apply_overrides,
)

DATA = Path(__file__).parent / "data"
GDX = DATA / "remind2pypsa_amt_filtered.gdx"
REF_RAW = DATA / "reference" / "costs_raw_overwritten.csv"


def test_build_overrides_maps_and_dedups():
    technologies = {
        "electrolysis": {"overrides": {"investment": "IAM", "efficiency": "IAM"}},
    }
    remind_long = pd.DataFrame(
        {
            "region": ["DEU", "DEU"],
            "technology": ["electrolysis", "electrolysis"],
            "parameter": ["investment", "efficiency"],
            "value": [728594.28, 0.73],
            "unit": ["USD/MW", "p.u."],
        }
    )
    out = build_iam_techdata(technologies, remind_long, source="TEST")
    assert set(out["technology"]) == {"electrolysis"}
    assert len(out) == 2


def test_build_overrides_raises_on_missing_iam_data():
    """An 'IAM'-declared parameter with no matching adapter row is a hard failure."""
    technologies = {"electrolysis": "IAM"}
    remind_long = pd.DataFrame(
        {"region": ["DEU"], "technology": ["electrolysis"], "parameter": ["investment"],
         "value": [1.0], "unit": ["USD/MW"]}
    )
    with pytest.raises(ValueError, match="no matching data"):
        build_iam_techdata(technologies, remind_long, source="TEST")


def test_build_baseline_overrides_drops_parameters_missing_from_baseline():
    """A 'PyPSA'-declared parameter absent from the baseline is skipped, not NaN-filled.

    PHS has no 'VOM' row in real PyPSA-Eur cost tables; that's a structurally-expected gap
    filled later via prepare_costs's fill_values, not a value/unit worth propagating as NaN.
    """
    technologies = {"PHS": "PyPSA"}
    baseline_raw = pd.DataFrame(
        {
            "technology": ["PHS"],
            "parameter": ["investment"],
            "value": [1000.0],
            "unit": ["USD/MW"],
        }
    )
    out = build_pypsa_techdata(technologies, baseline_raw)
    assert set(out["parameter"]) == {"investment"}
    assert not out[["value", "unit"]].isna().any().any()


def test_bare_string_shorthand_and_source_plus_overrides():
    """gas-ccgt: IAM is shorthand; entries with source:+overrides: resolve correctly."""
    from iampypsa.io.technology_mapping import iam_name, build_technology_sources

    assert build_technology_sources("IAM") == {
        p: "IAM" for p in
        ("investment", "FOM", "VOM", "efficiency", "lifetime", "fuel", "CO2 intensity")
    }

    spec = {"iam_name": "solar-pv", "source": "IAM", "overrides": {"fuel": {"value": 0}}}
    assert iam_name("solar", spec) == "solar-pv"
    sources = build_technology_sources(spec)
    assert sources["investment"] == "IAM"
    assert sources["fuel"] == {"value": 0}

    # No `source:` — only explicitly-listed parameters are sourced at all.
    partial = {"overrides": {"investment": "PyPSA", "FOM": "PyPSA"}}
    assert build_technology_sources(partial) == {"investment": "PyPSA", "FOM": "PyPSA"}


def test_load_technology_parameters_reads_file_directly(tmp_path):
    """load_technology_parameters is a plain YAML read — no merge, no renamed_from magic."""
    from iampypsa.io import load_technology_parameters

    config = tmp_path / "technology_parameters.yaml"
    config.write_text(
        "gas-ocgt: IAM\n"
        "solar:\n"
        "  iam_name: solar-pv\n"
        "  source: IAM\n"
    )
    technologies = load_technology_parameters(str(config))["technologies"]
    assert technologies == {
        "gas-ocgt": "IAM",
        "solar": {"iam_name": "solar-pv", "source": "IAM"},
    }


def test_broadcast_fuel_prices_zero_fills_technologies_outside_the_map():
    """A modeled technology absent from tech_fuel_map gets a real fuel: 0 row, not a gap."""
    df = pd.DataFrame(
        {
            "region": ["DEU", "DEU", "DEU", "DEU"],
            "technology": ["coal-fuel", "coal-pulverised", "coal-pulverised", "solar-pv"],
            "parameter": ["fuel", "investment", "efficiency", "investment"],
            "value": [10.0, 500.0, 0.4, 300.0],
            "unit": ["USD/MWh_th", "USD/MW", "p.u.", "USD/MW"],
        }
    )
    out = broadcast_fuel_prices(df, {"coal-pulverised": "coal-fuel"})

    coal_fuel = out.query("technology=='coal-pulverised' and parameter=='fuel'")
    assert coal_fuel["value"].iloc[0] == 10.0  # real broadcast value, unaffected

    solar_fuel = out.query("technology=='solar-pv' and parameter=='fuel'")
    assert len(solar_fuel) == 1
    assert solar_fuel["value"].iloc[0] == 0.0  # not in tech_fuel_map -> real 0, not missing

    # the raw per-fuel pseudo-technology row ("coal-fuel") is dropped, not left dangling
    assert "coal-fuel" not in set(out["technology"])


def test_merge_overrides_replaces_and_appends():
    baseline = pd.DataFrame(
        {"technology": ["CCGT"], "parameter": ["investment"], "value": [1.0], "unit": ["x"]}
    )
    overrides = pd.DataFrame(
        {
            "technology": ["CCGT", "electrolysis"],
            "parameter": ["investment", "investment"],
            "value": [2.0, 5.0],
            "unit": ["x", "x"],
        }
    )
    merged = apply_overrides(baseline, overrides)
    assert merged.query("technology=='CCGT'")["value"].iloc[0] == 2.0  # replaced
    assert merged.query("technology=='electrolysis'")["value"].iloc[0] == 5.0  # appended


def test_add_discount_rate_only_where_missing():
    costs = pd.DataFrame(
        {"technology": ["a", "a", "b"], "parameter": ["investment", "discount rate", "investment"],
         "value": [1.0, 0.05, 2.0], "unit": ["", "", ""]}
    )
    out = add_discount_rate(costs, 0.07)
    # 'a' already had a discount rate; only 'b' gets the new 0.07 row
    added = out.query("parameter=='discount rate' and value==0.07")
    assert set(added["technology"]) == {"b"}


def test_convert_investment_basis_synthetic():
    costs = pd.DataFrame(
        {"technology": ["electrolysis", "electrolysis"], "parameter": ["investment", "efficiency"],
         "value": [1000.0, 0.5], "unit": ["", ""]}
    )
    out = convert_investment_to_input_capacity_basis(costs)
    inv = out.query("technology=='electrolysis' and parameter=='investment'")["value"].iloc[0]
    assert inv == pytest.approx(500.0)  # 1000 * 0.5**1


def test_investment_basis_matches_reference_for_electrolysis():
    from iampypsa.couplers.remind import RemindGdxCoupler
    from iampypsa.io import RemindLoader
    from iampypsa.io.remind_symbols import load_symbol_specs

    loader = RemindLoader(str(GDX))
    symbols = load_symbol_specs(backend=loader.backend)
    coupler = RemindGdxCoupler(loader, symbols, region_map={}, config={}, model_regions=["DEU"])
    remind_long = coupler.extract_cost_parameters(2090)
    electrolysis = remind_long.query("region=='DEU' and technology=='electrolysis'")

    costs = electrolysis.query("parameter in ('investment', 'efficiency')")[["technology", "parameter", "value", "unit"]]
    got = convert_investment_to_input_capacity_basis(costs)
    got_inv = got.query("parameter=='investment'")["value"].iloc[0]

    ref = pd.read_csv(REF_RAW).query(
        "region=='DEU' and technology=='electrolysis' and parameter=='investment'"
    )["value"].iloc[0]
    assert got_inv == pytest.approx(ref)
