"""Convert REMIND sectoral electricity demand to annual PyPSA loads.

Ported from PyPSA-Eur ``import_REMIND_demand.py``. Name-agnostic: operates on an
already-loaded frame with ``[year, region, sector, value]`` (the adapter handles the
symbol choice + fallback, e.g. ``["v32_load_sector", "p32_load_sector"]``, via the loader).
"""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd

from rpycpl.units import unit_factor

TWA_TO_MWH = unit_factor("TWa", "MWh")


def convert_loads(
    raw: pd.DataFrame,
    *,
    unit_factor: float = TWA_TO_MWH,
    regions: Sequence[str] | None = None,
    year_col: str = "year",
    region_col: str = "region",
    sector_col: str = "sector",
    value_col: str = "value",
    unit_label: str = "MWh_el",
) -> pd.DataFrame:
    """Convert REMIND demand to annual MWh, one tidy row per (year, region, sector)."""
    
    df = raw[[year_col, region_col, sector_col, value_col]].copy()
    df[value_col] = df[value_col] * unit_factor
    df["unit"] = unit_label
    if regions is not None:
        df = df[df[region_col].isin(set(regions))]
    return (
        df.groupby([year_col, region_col, sector_col, "unit"], as_index=False, observed=True)[
            value_col
        ]
        .sum()
        .sort_values([year_col, region_col, sector_col])
        .reset_index(drop=True)
    )
