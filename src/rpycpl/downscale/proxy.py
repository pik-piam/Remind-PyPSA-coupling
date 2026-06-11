"""Build proxy (reference-distribution) shares from SSP population/GDP projections.

Ported from PyPSA-Eur ``downscale_REMIND_demand.py`` (``_normalize`` / ``_compute_weights``).
A *proxy* is the reference distribution used to split a coarse value across finer members;
here it is a sector-specific GDP/population blend from the SSP database.
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_AC_WEIGHTS = {"gdp": 0.6, "population": 0.4}


def normalise(s: pd.Series) -> pd.Series:
    """Normalise to sum 1; return uniform shares if the total is non-positive."""
    s = s.astype(float).clip(lower=0.0)
    total = s.sum()
    if total <= 0.0:
        return pd.Series(1.0 / len(s), index=s.index) if len(s) else s
    return s / total


def build_ssp_shares(
    members: list[str],
    year: int,
    sector: str,
    pop_data: pd.DataFrame,
    gdp_data: pd.DataFrame,
    sector_weights: dict,
    configured_countries: set[str] | None = None,
) -> dict[str, float]:
    """Return ``{country: share}`` for a region's members, year, and sector.

    Shares are a sector-specific blend of normalised GDP and population. SSP years are
    clamped to the last available year. Missing SSP data for *configured* countries raises;
    unconfigured countries with missing data get zero weight.
    """
    if configured_countries is None:
        configured_countries = set(members)
    w = sector_weights.get(sector, sector_weights.get("AC", DEFAULT_AC_WEIGHTS))

    available_years = pop_data.index.get_level_values("year").unique()
    lookup_year = min(year, available_years.max())

    idx = pd.MultiIndex.from_product([members, [lookup_year]], names=["iso2", "year"])
    pop = pop_data.reindex(idx)["value"]
    gdp = gdp_data.reindex(idx)["value"]

    for label, series in [("population", pop), ("GDP", gdp)]:
        missing = [
            c
            for c in series[series.isna()].index.get_level_values("iso2")
            if c in configured_countries
        ]
        if missing:
            raise ValueError(f"SSP {label} data missing for {missing} in year {lookup_year}.")

    pop = pop.fillna(0.0)
    gdp = gdp.fillna(0.0)
    pop.index = pop.index.get_level_values("iso2")
    gdp.index = gdp.index.get_level_values("iso2")

    weights = w["gdp"] * normalise(gdp) + w["population"] * normalise(pop)
    return normalise(weights).to_dict()
