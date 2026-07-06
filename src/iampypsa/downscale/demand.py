"""Disaggregate IAM regional demand to country level (region→country, Stage 1).

Single-member regions are a no-op; multi-member regions are split by SSP GDP/population
shares. Demand attributed to unconfigured countries is dropped (with a warning above 1% of
regional demand).
"""

from __future__ import annotations

import logging

import pandas as pd

from iampypsa.downscale.proxy import build_ssp_shares

logger = logging.getLogger(__name__)


def disaggregate_demand_to_country(
    sectoral_load: pd.DataFrame,
    region_to_countries: dict[str, list[str]],
    pop_data: pd.DataFrame,
    gdp_data: pd.DataFrame,
    sector_weights: dict,
    configured_countries: set[str],
) -> pd.DataFrame:
    """Split each (year, region, sector) row into per-country rows; return a tidy frame."""
    rows: list[dict] = []
    warned: set[str] = set()

    for _, row in sectoral_load.iterrows():
        region = row["region"]
        members = region_to_countries.get(region)
        if not members:
            logger.warning("IAM region '%s' not in region mapping — skipping.", region)
            continue
        configured = [c for c in members if c in configured_countries]
        if not configured:
            continue

        if len(members) == 1:
            rows.append({**row.to_dict(), "region": configured[0]})
            continue

        weights = build_ssp_shares(
            members, int(row["year"]), row["sector"], pop_data, gdp_data,
            sector_weights, configured_countries=configured_countries,
        )
        unconfigured = [c for c in members if c not in configured_countries]
        if unconfigured and region not in warned:
            frac = sum(weights.get(c, 0.0) for c in unconfigured)
            logger.warning(
                "IAM region '%s' has unconfigured countries %s (%.1f%% of demand) — excluded.",
                region, unconfigured, frac * 100,
            )
            warned.add(region)
        for country in configured:
            rows.append({**row.to_dict(), "region": country,
                         "value": row["value"] * weights.get(country, 0.0)})

    result = pd.DataFrame(rows)
    if result.empty:
        return result
    return (
        result.groupby(["year", "region", "sector", "unit"], as_index=False)["value"]
        .sum()
        .sort_values(["year", "region", "sector"])
        .reset_index(drop=True)
    )
