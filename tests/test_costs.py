"""Tests for shared cost-override mechanics (synthetic + real EUR GDX/reference)."""

from __future__ import annotations

import os

import pandas as pd
import pytest

from rpycpl.transforms.costs import (
    add_discount_rate,
    build_mapped_overrides,
    convert_investment_to_input_capacity_basis,
    apply_overrides,
)

DEV = "/workspace/remind_pypsa_coupling/development_data"
EUR_GDX = f"{DEV}/PkBudg1000_Europe_without_NES_fixed/i1/REMIND2PyPSAEUR.gdx"
REF_RAW = f"{DEV}/PkBudg1000_Europe_without_NES_fixed/i1/y2050/costs_raw_overwritten.csv"
MAP = "/workspace/pypsa-eur-aod/pypsa-eur/config/technology_cost_mapping.csv"


def test_build_overrides_maps_and_dedups():
    tech_map = pd.DataFrame(
        {
            "PyPSA-Eur technology": ["electrolysis", "electrolysis"],
            "reference": ["elh2", "elh2"],
            "parameter": ["investment", "efficiency"],
            "source": ["REMIND", "REMIND"],
        }
    )
    remind_long = pd.DataFrame(
        {
            "region": ["DEU", "DEU"],
            "reference": ["elh2", "elh2"],
            "parameter": ["investment", "efficiency"],
            "value": [728594.28, 0.73],
            "unit": ["USD/MW", "p.u."],
        }
    )
    out = build_mapped_overrides(
        tech_map, remind_long,
        tech_col="PyPSA-Eur technology", ref_col="reference",
        param_col="parameter", source_col="source", model_value="REMIND", out_source="TEST",
    )
    assert set(out["technology"]) == {"electrolysis"}
    assert len(out) == 2


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


@pytest.mark.skipif(not (os.path.exists(EUR_GDX) and os.path.exists(REF_RAW)),
                    reason="EUR development data not present")
def test_investment_basis_matches_reference_for_electrolysis():
    from rpycpl.io import read_gdx_symbol as read_gdx

    capcost = read_gdx(
        EUR_GDX, "p32_capCost",
        rename_columns={"ttot": "year", "all_regi": "region", "all_te": "technology"},
    ).query("region=='DEU' and year=='2050' and technology=='elh2'")["value"].iloc[0]

    costs = pd.DataFrame(
        {"technology": ["electrolysis", "electrolysis"], "parameter": ["investment", "efficiency"],
         "value": [capcost * 1e6, 0.73], "unit": ["USD/MW", "p.u."]}
    )
    got = convert_investment_to_input_capacity_basis(costs)
    got_inv = got.query("parameter=='investment'")["value"].iloc[0]

    ref = pd.read_csv(REF_RAW).query(
        "region=='DEU' and technology=='electrolysis' and parameter=='investment'"
    )["value"].iloc[0]
    assert got_inv == pytest.approx(ref)
