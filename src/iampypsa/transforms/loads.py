"""Convert IAM sectoral electricity demand to annual PyPSA loads.

Name-agnostic: operates on an already-loaded frame with ``[year, region, sector, value]``
(the Coupler handles the symbol choice + fallback via the loader).
"""

from collections.abc import Sequence

import pandas as pd


def convert_loads(
    raw: pd.DataFrame,
    *,
    regions: Sequence[str] | None = None,
    year_col: str = "year",
    region_col: str = "region",
    sector_col: str = "sector",
    value_col: str = "value",
    unit_label: str = "MWh_el",
) -> pd.DataFrame:
    """Label and reduce already-converted IAM demand to one row per (year, region, sector).

    Assumes ``raw`` is already in the target unit (conversion happens at the ``load_frame``
    seam); sums rows sharing a key as a guard against an unexpected extra source dimension.
    """
    df = raw[[year_col, region_col, sector_col, value_col]].copy()
    df["unit"] = unit_label
    if regions is not None:
        df = df[df[region_col].isin(set(regions))]
    return (
        df.groupby([year_col, region_col, sector_col, "unit"], as_index=False, observed=True)[
            value_col
        ]
        .sum()
        .sort_values([year_col, region_col, sector_col])
        [[year_col, region_col, sector_col, value_col, "unit"]]
        .reset_index(drop=True)
    )
