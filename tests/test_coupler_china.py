"""Smoke test for the PyPSA-China adapter against the raw China GDX.

No PyPSA-China reference outputs exist in the dev set, so this only checks that the adapter
resolves the CHA symbols and produces CO2 prices + config overrides from the real GDX.
"""

import importlib.util
import os

import pandas as pd
import pytest

ADAPTER = "/workspace/PyPSA-China-PIK/workflow/scripts/remind/adapter_remind_china.py"
CHINA_GDX = "/workspace/remind_pypsa_coupling/development_data/REMIND_china_example.gdx"
HAVE = os.path.exists(ADAPTER) and os.path.exists(CHINA_GDX)
pytestmark = pytest.mark.skipif(not HAVE, reason="China adapter or GDX not present")


def _adapter():
    from iampypsa.io import RemindLoader
    from iampypsa.io.remind_symbols import load_symbol_specs

    spec = importlib.util.spec_from_file_location("adapter_remind_china", ADAPTER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.RemindChinaAdapter(
        loader=RemindLoader(CHINA_GDX),
        symbols=load_symbol_specs("CHA"),
        region_map={"CHA": ["CN"]},
        config={"planning_horizons": [2030, 2050], "currency_factor": 1.0,
                "sector_weights": {}, "countries": ["CN"]},
        model_regions=["CHA"],
    )


def test_china_co2_prices_load_from_gdx():
    co2 = _adapter().build_co2_prices()
    assert {"region", "year", "value"} <= set(co2.columns)
    assert (co2["region"] == "CHA").all()
    assert len(co2) == 2  # CHA x [2030, 2050]


def test_china_config_overrides_read_scalars():
    ov = _adapter().build_config_overrides()
    assert ov["run"]["is_remind_coupled"] is True
    assert "CHA" in ov["run"]["remind"]["run_name"]  # SSP2-PkBudg1000_CHAbrown...
    assert ov["scenario"]["planning_horizons"] == [2030, 2050]


def test_china_historical_calibration_overrides_values():
    adapter = _adapter()
    loads = pd.DataFrame({"year": [2030, 2030], "region": ["CN", "CN"],
                          "sector": ["AC", "EV_pass"], "value": [1.0, 2.0], "unit": ["MWh_el"] * 2})
    adapter.config["historical_calibration"] = [{"year": 2030, "sector": "AC", "value": 999.0}]
    out = adapter.apply_historical_calibration(loads)
    assert out.query("sector == 'AC'")["value"].iloc[0] == 999.0
    assert out.query("sector == 'EV_pass'")["value"].iloc[0] == 2.0  # untouched
