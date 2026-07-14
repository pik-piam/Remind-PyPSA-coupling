"""Tests for the CO2 price transform, against synthetic data and the real EUR GDX."""

from pathlib import Path

import pandas as pd
import pytest

from iampypsa.transforms.co2_prices import (
    TONNE_C_TO_TONNE_CO2,
    convert_co2_prices,
    extract_co2_prices,
)

GDX = Path(__file__).parent / "data" / "remind2pypsa_amt_filtered.gdx"


def _raw() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "region": ["DEU", "DEU", "FRA", "CHA"],
            "year": ["2030", "2040", "2030", "2030"],
            "value": [100.0, 200.0, 50.0, 10.0],
        }
    )


def test_extract_filters_regions_and_casts_year():
    out = extract_co2_prices(_raw(), regions=["DEU", "FRA"])
    assert set(out["region"]) == {"DEU", "FRA"}
    assert out["year"].dtype.kind == "i"
    assert list(out.columns) == ["region", "year", "value"]


def test_extract_reindexes_to_full_grid_with_zeros():
    out = extract_co2_prices(_raw(), regions=["DEU", "FRA"], years=[2030, 2040])
    # FRA-2040 is missing in the raw frame -> filled with 0
    fra_2040 = out.query("region == 'FRA' and year == 2040")["value"].iloc[0]
    assert fra_2040 == 0.0
    assert len(out) == 4  # 2 regions x 2 years


def test_convert_applies_carbon_and_currency_factor():
    out = convert_co2_prices(_raw(), currency_factor=0.9, carbon_to_co2=True)
    expected = 100.0 * 0.9 * TONNE_C_TO_TONNE_CO2
    assert out["value"].iloc[0] == pytest.approx(expected)


def test_against_real_amt_gdx():
    from iampypsa.io import read_gdx_symbol as read_gdx

    raw = read_gdx(str(GDX), "p_priceCO2", rename_columns={"tall": "year", "all_regi": "region"})
    prices = convert_co2_prices(
        extract_co2_prices(raw, regions=["DEU", "EWN"], years=[2090, 2100]),
        currency_factor=1.0,
    )
    assert len(prices) == 4  # 2 regions x 2 years
    assert (prices["value"] >= 0).all()
    # converted prices are the carbon-price values scaled by the molar factor
    assert prices["value"].max() == pytest.approx(
        raw.assign(year=raw.year.astype(int))
        .query("region in ['DEU','EWN'] and year in [2090,2100]")["value"].max()
        * TONNE_C_TO_TONNE_CO2
    )
