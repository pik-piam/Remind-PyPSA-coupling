"""Tests for the shared capacity-target transforms (synthetic + real EUR data).

Validates the generator-carrier pipeline against installed_capacities.csv. Carriers that
need spec-driven consolidation (VRE merge, battery scaling, link η-adjustment) are exercised
in the consolidation tests below.
"""

from __future__ import annotations

import os

import pandas as pd
import pytest

from rpycpl.transforms.capacities import (
    adjust_link_capacities_to_input,
    aggregate_capacities_to_carriers,
    apply_consolidation,
)

DEV = "/workspace/remind_pypsa_coupling/development_data/PkBudg1000_Europe_without_NES_fixed/i1"
EUR_GDX = f"{DEV}/REMIND2PyPSAEUR.gdx"
INSTALLED = f"{DEV}/installed_capacities.csv"
MAP = "/workspace/pypsa-eur-aod/pypsa-eur/config/technology_cost_mapping.csv"

# Pure generators whose target = p32_capAvg*1e6 mapped 1:1 (no VRE/battery/link prep).
GENERATOR_CARRIERS = ["ccgt", "ocgt", "onwind", "offwind", "solar", "nuclear"]


def test_adjust_link_capacities_divides_by_efficiency():
    caps = pd.DataFrame({"year": [2050, 2050], "region": ["DEU", "DEU"],
                         "technology": ["elh2", "ngcc"], "value": [100.0, 100.0]})
    eff = pd.DataFrame({"year": [2050, 2050], "region": ["DEU", "DEU"],
                        "technology": ["elh2", "ngcc"], "efficiency": [0.5, 0.6]})
    out = adjust_link_capacities_to_input(caps, eff, link_techs={"elh2"}).set_index("technology")["value"]
    assert out["elh2"] == pytest.approx(200.0)   # 100 / 0.5
    assert out["ngcc"] == pytest.approx(100.0)    # not a link tech -> unchanged


def test_aggregate_sums_and_filters():
    caps = pd.DataFrame({"year": [2050, 2050], "region": ["DEU", "DEU"],
                         "technology": ["spv", "csp"], "value": [10.0, 5.0]})
    tmap = pd.DataFrame({"model_tech": ["spv", "csp"], "target_carrier": ["solar", "solar"]})
    out = aggregate_capacities_to_carriers(
        caps, tmap, map_tech_col="model_tech", map_carrier_col="target_carrier"
    )
    assert out.query("carrier=='solar'")["value"].iloc[0] == pytest.approx(15.0)
    assert out.query("carrier=='solar'")["unit"].iloc[0] == "MW"


def test_apply_consolidation_noop_without_params():
    """A config with no consolidation block (e.g. IAMC) leaves capacities untouched."""
    caps = pd.DataFrame({"year": [2050, 2050], "region": ["DEU", "DEU"],
                         "technology": ["elh2VRE", "storspv"], "value": [10.0, 5.0]})
    out = apply_consolidation(caps)
    pd.testing.assert_frame_equal(out, caps)


def test_apply_consolidation_merges_vre_and_scales_battery():
    """Consolidation block: elh2VRE→elh2 merge and storX→btin scaling."""
    caps = pd.DataFrame(
        {"year": [2050, 2050, 2050], "region": ["DEU", "DEU", "DEU"],
         "technology": ["elh2VRE", "storspv", "storwindon"], "value": [10.0, 5.0, 2.0]})
    out = apply_consolidation(
        caps,
        vre_to_primary={"elh2VRE": "elh2"},
        battery_scaling={"storspv": 4.0, "storwindon": 1.2},
    ).set_index("technology")["value"]
    assert out.loc["elh2"] == pytest.approx(10.0)
    assert out.loc["btin"].sum() == pytest.approx(5.0 * 4.0 + 2.0 * 1.2)


def test_apply_consolidation_uses_btin_directly_when_present():
    """Bidirectional guard: an explicit btin capacity is kept; storX rows are dropped."""
    caps = pd.DataFrame(
        {"year": [2050, 2050], "region": ["DEU", "DEU"],
         "technology": ["btin", "storspv"], "value": [7.0, 5.0]})
    out = apply_consolidation(caps, battery_scaling={"storspv": 4.0}).set_index("technology")["value"]
    assert out["btin"] == pytest.approx(7.0)
    assert "storspv" not in out.index


def test_build_capacity_targets_reads_consolidation_from_symbols():
    """build_capacity_targets applies the consolidation block declared in the capacity spec."""
    from rpycpl.transforms.capacities import build_capacity_targets

    class _FakeLoader:
        def load_symbol(self, ref, rename_columns=None):
            if ref == "p32_capAvg":
                return pd.DataFrame({"year": [2050], "region": ["DEU"],
                                     "technology": ["storspv"], "value": [5.0]})
            return pd.DataFrame({"year": [], "region": [], "technology": [], "value": []})

        def resolve_symbol(self, ref):
            return ref

    symbols = {
        "capacity": {
            "symbol": "p32_capAvg",
            "consolidation": {"battery_scaling": {"storspv": 4.0}, "link_techs": []},
        },
    }
    tmap = pd.DataFrame({"model_tech": ["btin"], "target_carrier": ["battery charger"]})
    out = build_capacity_targets(
        _FakeLoader(), symbols, ["DEU"], tmap,
        map_tech_col="model_tech", map_carrier_col="target_carrier",
    )
    assert out.query("carrier == 'battery charger'")["value"].iloc[0] == pytest.approx(5.0 * 4.0)


@pytest.mark.skipif(not (os.path.exists(EUR_GDX) and os.path.exists(INSTALLED) and os.path.exists(MAP)),
                    reason="EUR development data not present")
def test_generator_targets_match_reference():
    from rpycpl.io import read_gdx_symbol as read_gdx

    raw = read_gdx(EUR_GDX, "p32_capAvg",
                   rename_columns={"ttot": "year", "all_regi": "region", "all_te": "technology"})
    raw["year"] = raw["year"].astype(int)
    # Unit conversion (TW→MW) is declared in the GDX symbol spec; apply here directly.
    raw = raw[["year", "region", "technology", "value"]].copy()
    raw["value"] *= 1e6

    mapping = pd.read_csv(MAP).query("parameter == 'investment' and source == 'REMIND'")
    tmap = mapping[["PyPSA-Eur technology", "reference"]].rename(
        columns={"PyPSA-Eur technology": "PyPSA-Eur", "reference": "REMIND-EU"}
    )

    got = aggregate_capacities_to_carriers(
        raw, tmap, map_tech_col="REMIND-EU", map_carrier_col="PyPSA-Eur"
    ).query("region == 'DEU' and year == 2050")
    ref = pd.read_csv(INSTALLED).query("region_REMIND == 'DEU' and year == 2050")

    got_s = got.set_index("carrier")["value"]
    ref_s = ref.set_index("carrier")["value"]
    checked = [c for c in GENERATOR_CARRIERS if c in ref_s.index and c in got_s.index]
    assert checked, "no generator carriers found to validate"
    for c in checked:
        assert got_s[c] == pytest.approx(ref_s[c], rel=1e-6), f"{c}: {got_s[c]} != {ref_s[c]}"
