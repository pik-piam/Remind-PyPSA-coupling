"""Tests for RemindGdxCoupler's sparse-GDX-reading helpers."""

import pandas as pd

from iampypsa.models.remind import RemindGdxCoupler


def test_fill_missing_with_zero_fills_only_modeled_technologies():
    """A technology present in modeled_techs but absent from the sparse frame gets 0; a
    technology absent from modeled_techs entirely is not invented."""
    modeled_techs = pd.DataFrame(
        {"region": ["DEU", "DEU", "FRA"], "technology": ["spv", "ngt", "spv"]}
    )
    sparse = pd.DataFrame(
        {
            "region": ["DEU"], "technology": ["ngt"], "value": [0.42],
            "unit": ["t_CO2/MWh_th"],
        }
    )

    out = RemindGdxCoupler._fill_missing_with_zero(sparse, modeled_techs, "CO2 intensity")

    assert set(zip(out["region"], out["technology"])) == {
        ("DEU", "spv"), ("DEU", "ngt"), ("FRA", "spv"),
    }
    deu_ngt = out[(out["region"] == "DEU") & (out["technology"] == "ngt")]
    assert deu_ngt["value"].iloc[0] == 0.42  # real value preserved, not overwritten
    deu_spv = out[(out["region"] == "DEU") & (out["technology"] == "spv")]
    assert deu_spv["value"].iloc[0] == 0.0  # missing -> filled with 0
    assert (out["parameter"] == "CO2 intensity").all()
    assert (out["unit"] == "t_CO2/MWh_th").all()  # carried over from sparse, not hardcoded


def test_fill_missing_with_zero_no_op_when_sparse_already_complete():
    modeled_techs = pd.DataFrame({"region": ["DEU"], "technology": ["ngt"]})
    sparse = pd.DataFrame(
        {"region": ["DEU"], "technology": ["ngt"], "value": [1.23], "unit": ["USD/MWh"]}
    )

    out = RemindGdxCoupler._fill_missing_with_zero(sparse, modeled_techs, "VOM")

    assert len(out) == 1
    assert out["value"].iloc[0] == 1.23
    assert out["unit"].iloc[0] == "USD/MWh"
