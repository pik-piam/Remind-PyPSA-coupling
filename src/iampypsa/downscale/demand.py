"""Disaggregate IAM regional demand to country level (region→country, Stage 1).

Single-member regions are a no-op; multi-member regions are split by proxy shares — a
sector-specific blend of registered proxies (e.g. GDP/population for AC, heating/cooling
degree-day demand for heating/cooling; see ``downscale.proxy.build_proxy_shares``). Demand
attributed to unconfigured countries is dropped, with a warning naming them and their share.
"""

import logging

import pandas as pd

from iampypsa.downscale.proxy import build_proxy_shares

logger = logging.getLogger(__name__)


def disaggregate_demand_to_country(
    sectoral_load: pd.DataFrame,
    region_to_countries: dict[str, list[str]],
    proxies: dict[str, pd.DataFrame],
    sector_weights: dict,
    configured_countries: set[str],
) -> pd.DataFrame:
    """Split each (year, region, sector) row into per-country rows; return a long-format table.

    ``proxies`` is a name→frame registry (e.g. ``{"population": ..., "gdp": ...,
    "heating_demand": ..., "cooling_demand": ...}``); each sector's ``sector_weights`` entry names
    which proxies to blend, e.g.::

        sector_weights = {"AC": {"gdp": 0.6, "population": 0.4},
                          "heating": {"heating_demand": 1.0}}

    Both are passed straight to :func:`~iampypsa.downscale.proxy.build_proxy_shares`, whose
    docstring carries a worked example of the proxy frames' shape.
    """
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

        weights = build_proxy_shares(
            members, int(row["year"]), row["sector"], proxies,
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
        [["year", "region", "sector", "value", "unit"]]
        .reset_index(drop=True)
    )
