"""Tests for the CO2 price transform, against synthetic data and the real EUR GDX."""

from pathlib import Path

import pandas as pd
import pytest

from iampypsa.transforms.co2_prices import extract_co2_prices
from iampypsa.transforms.costs import apply_currency_factor
from iampypsa.units import TONNE_C_TO_TONNE_CO2

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


def test_currency_factor_scales_every_row_of_a_single_quantity_frame():
    """The CO2 price frame has no `parameter` column, so parameters=None scales all of it —
    the same seam the cost table uses with a parameter filter."""
    out = apply_currency_factor(_raw(), 0.9, parameters=None)
    assert out["value"].iloc[0] == pytest.approx(100.0 * 0.9)


def test_against_real_amt_gdx():
    """Load through the seam, as production does: the spec's USD/tC -> USD/tCO2 conversion is
    applied by load_frame, and the currency factor only rescales."""
    from iampypsa.io import RemindLoader, load_frame, load_symbol_specs, read_gdx_symbol

    loader = RemindLoader(str(GDX))
    raw = load_frame(loader, load_symbol_specs(backend="gdx")["co2_price"])
    prices = apply_currency_factor(
        extract_co2_prices(raw, regions=["DEU", "EWN"], years=[2090, 2100]),
        1.0,
        parameters=None,
    )
    assert len(prices) == 4  # 2 regions x 2 years
    assert (prices["value"] >= 0).all()

    # The seam applied the molar factor exactly once, against the untouched GDX values.
    gdx = read_gdx_symbol(str(GDX), "p_priceCO2", {"tall": "year", "all_regi": "region"})
    assert prices["value"].max() == pytest.approx(
        gdx.assign(year=gdx.year.astype(int))
        .query("region in ['DEU','EWN'] and year in [2090,2100]")["value"].max()
        * TONNE_C_TO_TONNE_CO2
    )
