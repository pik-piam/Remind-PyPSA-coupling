"""Tests for the shared capacity-target transforms (synthetic + real EUR data).

Validates the generator-carrier pipeline against installed_capacities.csv. Carriers that
need REMIND-interface-specific prep (VRE merge, battery scaling, link η-adjustment) are
done in the adapter and excluded from this generator-only check.
"""

from __future__ import annotations

import os

import pandas as pd
import pytest

from rpycpl.transforms.capacities import (
    adjust_link_capacities_to_input,
    aggregate_capacities_to_carriers,
    convert_capacities,
    prepare_capacities,
)

DEV = "/workspace/remind_pypsa_coupling/development_data/PkBudg1000_Europe_without_NES_fixed/i1"
EUR_GDX = f"{DEV}/REMIND2PyPSAEUR.gdx"
INSTALLED = f"{DEV}/installed_capacities.csv"
MAP = "/workspace/pypsa-eur-aod/pypsa-eur/config/technology_cost_mapping.csv"

# Pure generators whose target = p32_capAvg*1e6 mapped 1:1 (no VRE/battery/link prep).
GENERATOR_CARRIERS = ["ccgt", "ocgt", "onwind", "offwind", "solar", "nuclear"]


def test_convert_capacities_unit():
    raw = pd.DataFrame({"year": [2050], "region": ["DEU"], "technology": ["ngcc"], "value": [1.0]})
    assert convert_capacities(raw)["value"].iloc[0] == pytest.approx(1e6)


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
    tmap = pd.DataFrame({"REMIND-EU": ["spv", "csp"], "PyPSA-Eur": ["solar", "solar"]})
    out = aggregate_capacities_to_carriers(caps, tmap)
    assert out.query("carrier=='solar'")["p_nom_min"].iloc[0] == pytest.approx(15.0)


def test_prepare_capacities_noop_without_params():
    """An IAMC-style config (no consolidation block) leaves capacities untouched."""
    caps = pd.DataFrame({"year": [2050, 2050], "region": ["DEU", "DEU"],
                         "technology": ["elh2VRE", "storspv"], "value": [10.0, 5.0]})
    out = prepare_capacities(caps)
    pd.testing.assert_frame_equal(out, caps)


def test_prepare_capacities_merges_vre_and_scales_battery():
    """REMIND-GDX prep: elh2VRE→elh2 merge and storX→btin scaling."""
    caps = pd.DataFrame(
        {"year": [2050, 2050, 2050], "region": ["DEU", "DEU", "DEU"],
         "technology": ["elh2VRE", "storspv", "storwindon"], "value": [10.0, 5.0, 2.0]})
    out = prepare_capacities(
        caps,
        vre_to_primary={"elh2VRE": "elh2"},
        battery_scaling={"storspv": 4.0, "storwindon": 1.2},
    ).set_index("technology")["value"]
    assert out.loc["elh2"] == pytest.approx(10.0)           # variant merged to primary name
    # storX rows are renamed+scaled to btin (summed into one carrier later in aggregate)
    assert out.loc["btin"].sum() == pytest.approx(5.0 * 4.0 + 2.0 * 1.2)


def test_prepare_capacities_uses_btin_directly_when_present():
    """Bidirectional guard: an explicit btin capacity is kept; storX rows are dropped, not scaled."""
    caps = pd.DataFrame(
        {"year": [2050, 2050], "region": ["DEU", "DEU"],
         "technology": ["btin", "storspv"], "value": [7.0, 5.0]})
    out = prepare_capacities(caps, battery_scaling={"storspv": 4.0}).set_index("technology")["value"]
    assert out["btin"] == pytest.approx(7.0)
    assert "storspv" not in out.index


def test_build_capacity_targets_reads_consolidation_from_symbols():
    """build_capacity_targets pulls the prep + link techs from the capacity symbol spec."""
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
    tmap = pd.DataFrame({"REMIND-EU": ["btin"], "PyPSA-Eur": ["battery charger"]})
    out = build_capacity_targets(_FakeLoader(), symbols, ["DEU"], tmap)
    assert out.query("carrier == 'battery charger'")["p_nom_min"].iloc[0] == pytest.approx(5.0 * 4.0)


@pytest.mark.skipif(not (os.path.exists(EUR_GDX) and os.path.exists(INSTALLED) and os.path.exists(MAP)),
                    reason="EUR development data not present")
def test_generator_targets_match_reference():
    from rpycpl.io import read_gdx_symbol as read_gdx

    raw = read_gdx(EUR_GDX, "p32_capAvg",
                   rename_columns={"ttot": "year", "all_regi": "region", "all_te": "technology"})
    raw["year"] = raw["year"].astype(int)
    caps = convert_capacities(raw)

    mapping = pd.read_csv(MAP).query("parameter == 'investment' and source == 'REMIND'")
    tmap = mapping[["PyPSA-Eur technology", "reference"]].rename(
        columns={"PyPSA-Eur technology": "PyPSA-Eur", "reference": "REMIND-EU"}
    )

    got = aggregate_capacities_to_carriers(caps, tmap).query("region == 'DEU' and year == 2050")
    ref = pd.read_csv(INSTALLED).query("region_REMIND == 'DEU' and year == 2050")

    got_s = got.set_index("carrier")["p_nom_min"]
    ref_s = ref.set_index("carrier")["p_nom_min"]
    checked = [c for c in GENERATOR_CARRIERS if c in ref_s.index and c in got_s.index]
    assert checked, "no generator carriers found to validate"
    for c in checked:
        assert got_s[c] == pytest.approx(ref_s[c], rel=1e-6), f"{c}: {got_s[c]} != {ref_s[c]}"
