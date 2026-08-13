"""Tests for the shared capacity-target transforms (synthetic + real EUR data).

Validates the generator-carrier pipeline against installed_capacities.csv. Carriers that
need spec-driven postprocessing (token merge, scaling, link η-adjustment) are exercised
in the postprocessing tests below.
"""

from pathlib import Path

import pandas as pd
import pytest

from iampypsa.transforms.capacities import (
    adjust_link_capacities_to_input,
    aggregate_capacities_to_carriers,
    apply_postprocessing,
)

DATA = Path(__file__).parent / "data"
GDX = DATA / "remind2pypsa_amt_filtered.gdx"
INSTALLED = DATA / "reference" / "installed_capacities.csv"
TECH_MAPPING = DATA / "technology_mapping_example.yaml"

# Carriers present in both the example technology mapping and installed_capacities.csv.
GENERATOR_CARRIERS = ["gas-ocgt", "solar", "biomass-igcc-ccs"]


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


def test_aggregate_preserves_one_tech_feeding_several_carriers():
    """A REMIND tech mapped to several carriers (e.g. wind-offshore -> offwind, offwind-ac)
    must feed each carrier the full value, not just the first-listed one."""
    caps = pd.DataFrame({"year": [2050], "region": ["DEU"],
                         "technology": ["wind-offshore"], "value": [12.0]})
    tmap = pd.DataFrame({"model_tech": ["wind-offshore", "wind-offshore"],
                         "target_carrier": ["offwind", "offwind-ac"]})
    out = aggregate_capacities_to_carriers(
        caps, tmap, map_tech_col="model_tech", map_carrier_col="target_carrier"
    ).set_index("carrier")["value"]
    assert out["offwind"] == pytest.approx(12.0)
    assert out["offwind-ac"] == pytest.approx(12.0)


def test_apply_postprocessing_noop_without_params():
    """A config with no postprocessing block (e.g. IAMC) leaves capacities untouched."""
    caps = pd.DataFrame({"year": [2050, 2050], "region": ["DEU", "DEU"],
                         "technology": ["elh2VRE", "storspv"], "value": [10.0, 5.0]})
    out = apply_postprocessing(caps)
    pd.testing.assert_frame_equal(out, caps)


def test_apply_postprocessing_merges_and_scales():
    """Postprocessing block: elh2VRE→elh2 merge and storX→btin merge+scaling."""
    caps = pd.DataFrame(
        {"year": [2050, 2050, 2050], "region": ["DEU", "DEU", "DEU"],
         "technology": ["elh2VRE", "storspv", "storwindon"], "value": [10.0, 5.0, 2.0]})
    out = apply_postprocessing(
        caps,
        merge={"elh2": ["elh2", "elh2VRE"], "btin": ["btin", "storspv", "storwindon"]},
        scale={"storspv": 4.0, "storwindon": 1.2},
    ).set_index("technology")["value"]
    assert out.loc["elh2"] == pytest.approx(10.0)
    assert out.loc["btin"].sum() == pytest.approx(5.0 * 4.0 + 2.0 * 1.2)


def test_apply_postprocessing_uses_target_directly_when_present():
    """Bidirectional guard: an explicit btin capacity is kept; storX rows are dropped."""
    caps = pd.DataFrame(
        {"year": [2050, 2050], "region": ["DEU", "DEU"],
         "technology": ["btin", "storspv"], "value": [7.0, 5.0]})
    out = apply_postprocessing(
        caps, merge={"btin": ["btin", "storspv"]}, scale={"storspv": 4.0}
    ).set_index("technology")["value"]
    assert out["btin"] == pytest.approx(7.0)
    assert "storspv" not in out.index


def test_get_capacities_reads_postprocessing_from_symbols():
    """get_capacities applies the postprocessing block declared in the capacity spec."""
    from iampypsa.couplers.base import Coupler

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
            "postprocessing": {
                "merge": {"btin": ["btin", "storspv"]},
                "scale": {"storspv": 4.0},
                "link_techs": [],
            },
        },
    }
    tmap = pd.DataFrame({"model_tech": ["btin"], "target_carrier": ["battery charger"]})
    coupler = Coupler(_FakeLoader(), symbols, {}, {}, model_regions=["DEU"])
    out = coupler.get_capacities(
        tmap, map_tech_col="model_tech", map_carrier_col="target_carrier",
    )
    assert out.query("carrier == 'battery charger'")["value"].iloc[0] == pytest.approx(5.0 * 4.0)


def test_generator_targets_match_reference():
    from iampypsa.io import RemindLoader, build_capacity_reporting_technologies, load_technology_parameters
    from iampypsa.io.remind_symbols import load_symbol_specs
    from iampypsa.io.technology_mapping import iam_name
    from iampypsa.couplers.base import Coupler
    from iampypsa.io.remind_symbols import rename_technologies

    loader = RemindLoader(str(GDX))
    symbols = load_symbol_specs(backend=loader.backend)
    raw = Coupler(loader, symbols, {}, {}).prepare_capacities()
    raw = rename_technologies(raw, symbols.get("technology_names"))
    raw["year"] = raw["year"].astype(int)

    technology_mapping = load_technology_parameters(str(TECH_MAPPING))["technologies"]
    reports_capacity = build_capacity_reporting_technologies()
    tmap = pd.DataFrame(
        [
            {"PyPSA": tech, "IAM": iam_name(tech, spec)}
            for tech, spec in technology_mapping.items()
            if iam_name(tech, spec) in reports_capacity
        ]
    )

    got = aggregate_capacities_to_carriers(
        raw, tmap, map_tech_col="IAM", map_carrier_col="PyPSA"
    ).query("region == 'DEU' and year == 2090")
    ref = pd.read_csv(INSTALLED).query("region == 'DEU' and year == 2090")

    got_s = got.set_index("carrier")["value"]
    ref_s = ref.set_index("carrier")["value"]
    checked = [c for c in GENERATOR_CARRIERS if c in ref_s.index and c in got_s.index]
    assert checked, "no generator carriers found to validate"
    for c in checked:
        assert got_s[c] == pytest.approx(ref_s[c], rel=1e-6), f"{c}: {got_s[c]} != {ref_s[c]}"
