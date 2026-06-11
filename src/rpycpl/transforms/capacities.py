"""Build REMIND installed-capacity targets (p_nom_min) for PyPSA.

Ported from PyPSA-Eur ``import_REMIND_capacities.py``. These are the *shared* steps:
unit conversion, link output→input adjustment, and aggregation to PyPSA carriers. The
REMIND-interface-specific prep (VRE-variant merge, battery scaling — which depend on
REMIND tech names) lives in each model's adapter. Brownfield harmonisation / paid-off
capacity is handled separately in ``rpycpl.capacities_etl`` (China path).
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

import pandas as pd

logger = logging.getLogger(__name__)


# TODO where are unit conversion defined?
# TODO consider having an optional unit in the remind_symbols and a unit parser
def convert_capacities(
    raw: pd.DataFrame,
    *,
    unit_factor: float = 1e6,  # TW -> MW
    year_col: str = "year",
    region_col: str = "region",
    tech_col: str = "technology",
    value_col: str = "value",
) -> pd.DataFrame:
    """Convert REMIND capacities to MW and return a tidy ``[year, region, technology, value]``."""
    
    df = raw[[year_col, region_col, tech_col, value_col]].copy()
    df[value_col] = df[value_col] * unit_factor
    return df.reset_index(drop=True)


def adjust_link_capacities_to_input(
    capacities: pd.DataFrame,
    efficiencies: pd.DataFrame,
    link_techs: set[str],
    *,
    on: Sequence[str] = ("year", "region", "technology"),
    tech_col: str = "technology",
    value_col: str = "value",
    eff_col: str = "efficiency",
) -> pd.DataFrame:
    """Divide output-based capacities by efficiency for link-like techs (→ input basis).

    Rows with missing or zero efficiency are left unchanged (with a warning).
    """
    merged = capacities.merge(efficiencies, on=list(on), how="left")
    is_link = merged[tech_col].isin(link_techs)
    missing = is_link & merged[eff_col].isna()
    zero = is_link & (merged[eff_col] == 0)
    if missing.any():
        logger.warning("Missing efficiency for %d link rows; keeping originals.", int(missing.sum()))
    if zero.any():
        logger.warning("Zero efficiency for %d link rows; keeping originals.", int(zero.sum()))
    valid = is_link & merged[eff_col].notna() & (merged[eff_col] != 0)
    merged.loc[valid, value_col] = merged.loc[valid, value_col] / merged.loc[valid, eff_col]
    return merged.drop(columns=[eff_col])


def aggregate_capacities_to_carriers(
    capacities: pd.DataFrame,
    tech_to_carrier: pd.DataFrame,
    *,
    group_cols: Sequence[str] = ("year", "region"),
    tech_col: str = "technology",
    map_tech_col: str = "REMIND-EU",
    map_carrier_col: str = "PyPSA-Eur",
    value_col: str = "value",
    min_value: float = 0.0,
    round_digits: int = 2,
) -> pd.DataFrame:
    """Map REMIND techs to PyPSA carriers, sum per (group, carrier), and return ``p_nom_min``.
    
    This is needed in case mapping is not 1:1"""
    carrier_map = tech_to_carrier[[map_tech_col, map_carrier_col]].drop_duplicates(
        subset=map_tech_col, keep="first"
    )
    mapped = capacities.merge(carrier_map, left_on=tech_col, right_on=map_tech_col, how="left")
    unmapped = mapped[map_carrier_col].isna().sum()
    if unmapped:
        logger.warning("Dropping %d rows with unmapped REMIND technologies.", int(unmapped))
    mapped = mapped.dropna(subset=[map_carrier_col]).rename(columns={map_carrier_col: "carrier"})

    grouped = (
        mapped.groupby([*group_cols, "carrier"], as_index=False, observed=True)[value_col]
        .sum()
        .round(round_digits)
    )
    grouped = grouped[grouped[value_col] > min_value].rename(columns={value_col: "p_nom_min"})
    return grouped.sort_values([*group_cols, "carrier"]).reset_index(drop=True)
