"""Build proxy (reference-distribution) shares from named proxy frames.

A *proxy* is the reference distribution used to split a coarse IAM regional value across
country-level members.  Proxies are supplied as a name→frame registry
(``{"population": ..., "gdp": ..., "heating_demand": ..., "cooling_demand": ...}``); each proxy
frame is a ``MultiIndex[(iso2, year)]`` with a ``value`` column (the shape produced by
``io.ssp.read_ssp_data`` and — via :func:`build_demand_proxy_from_dd` — ``io.degree_days.read_degree_days``).
A sector's weight dict names which proxies to blend and with what weight (e.g.
``{"gdp": 0.6, "population": 0.4}`` for AC, ``{"heating_demand": 1.0}`` for heating,
``{"cooling_demand": 1.0}`` for cooling). No proxy is privileged.

The heating/cooling proxies are *demand* distributions (``population × HDD`` / ``population × CDD``,
built by :func:`build_demand_proxy_from_dd`) — not raw degree-days — hence the ``*_demand`` names: raw
degree-days are an intensity and would over-allocate to small hot/cold countries.
"""

from __future__ import annotations

import logging

import pandas as pd
from typing_extensions import deprecated

logger = logging.getLogger(__name__)

DEFAULT_AC_WEIGHTS = {"gdp": 0.6, "population": 0.4}


def build_demand_proxy_from_dd(
    degree_days: pd.DataFrame, population: pd.DataFrame, *, value_col: str = "value"
) -> pd.DataFrame:
    """Build an *extensive* heating/cooling demand proxy from degree-days × population.

    Degree-days are an *intensity*, so splitting demand by raw HDD/CDD over-allocates to small
    hot/cold countries. Multiplying by population (an extensive size variable) yields a
    demand-distribution proxy — ``population × HDD`` for heating, ``population × CDD`` for cooling —
    which is what belongs in the ``proxies`` registry (keyed ``heating_demand`` / ``cooling_demand``).

    Both frames are ``MultiIndex[(iso2, year)]`` with a ``value`` column. For each ``(iso2, year)``
    in ``degree_days``, ``population`` is taken at the **nearest available year** for that country
    (so a single-year degree-day frame pairs with the closest population year). Countries absent
    from ``population`` are dropped. Returns the same ``MultiIndex[(iso2, year)]`` shape.

    Args:
        degree_days (pd.DataFrame): The intensive degree-day frame (HDD or CDD).
        population (pd.DataFrame): The extensive population frame to weight by.
        value_col (str): The column name in both frames to multiply.
    Returns:
        pd.DataFrame: The extensive heating/cooling demand proxy frame.
    """
    b = degree_days.reset_index()[["iso2", "year", value_col]].sort_values("year")
    w = population.reset_index()[["iso2", "year", value_col]].sort_values("year")
    merged = pd.merge_asof(
        b, w, on="year", by="iso2", direction="nearest", suffixes=("", "_w")
    )
    merged[value_col] = merged[value_col] * merged[f"{value_col}_w"]
    return (
        merged.dropna(subset=[value_col])[["iso2", "year", value_col]]
        .set_index(["iso2", "year"])
    )


def normalise(s: pd.Series) -> pd.Series:
    """Normalise to sum 1; return uniform shares if the total is non-positive."""
    s = s.astype(float).clip(lower=0.0)
    total = s.sum()
    if total <= 0.0:
        return pd.Series(1.0 / len(s), index=s.index) if len(s) else s
    return s / total


def build_proxy_shares(
    members: list[str],
    year: int,
    sector: str,
    proxies: dict[str, pd.DataFrame],
    sector_weights: dict,
    configured_countries: set[str] | None = None,
) -> dict[str, float]:
    """Return ``{country: share}`` for the region's members, selected year, and sector.

    Shares are a weighted blend of the normalised proxy frames named in the sector's weight dict
    ``sector_weights[sector]``. Proxy years are clamped **per proxy** to that
    proxy's last available year (so degree-day frames covering fewer years clamp independently of
    population/GDP). 
    
    Missing proxy data for *configured* countries raises; unconfigured countries
    with missing data get zero weight.

    Args:
        members (list[str]): The region's member countries (ISO2).
        year (int): The scenario year.
        sector (str): The sector name (e.g. "AC").
        proxies (dict[str, pd.DataFrame]): Name→frame registry of proxy frames.
        sector_weights (dict): Sector→proxy-weight dict.
        configured_countries (set[str] | None): Countries to include in the shares; if ``None``,
            all ``members`` are included. Missing proxy data for configured countries raises.
    Raises:
        ValueError: If a sector requests a proxy that is not present in ``proxies`
        ValueError: If a configured country is missing proxy data for the selected year.
    Returns:
        dict[str, float]: The normalised shares for the configured countries.
    Example:
        >>> proxies = {
        ...     "population": pd.DataFrame(
        ...         {"value": [10, 20, 30, 40]},
        ...         index=pd.MultiIndex.from_tuples(
        ...             [("DE", 2020), ("FR", 2020), ("DE", 2030), ("FR", 2030)],
        ...             names=["iso2", "year"],
        ...         ),
        ...     ),
        ...     "gdp": pd.DataFrame(
        ...         {"value": [100, 200, 300, 400]},
        ...         index=pd.MultiIndex.from_tuples(
        ...             [("DE", 2020), ("FR", 2020), ("DE", 2030), ("FR", 2030)],
        ...             names=["iso2", "year"],
        ...         ),
        ...     ),
        ... }
        >>> sector_weights = {"AC": {"population": 0.4, "gdp": 0.6}}"
    """
    if configured_countries is None:
        configured_countries = set(members)
    
    if not sector in sector_weights:
        raise ValueError(
            f"Sector '{sector}' not present in sector_weights; available sectors: "
            f"{sorted(sector_weights)}."
        )
    sec_w = sector_weights.get(sector)

    missing_proxies = [name for name in sec_w if name not in proxies]
    if missing_proxies:
        raise ValueError(
            f"Sector '{sector}' requests proxies {missing_proxies} that were not supplied "
            f"(available proxies: {sorted(proxies)})."
        )

    blended: pd.Series | None = None
    for name, weight in sec_w.items():
        frame = proxies[name]
        available_years = frame.index.get_level_values("year").unique()
        # Nearest available year: identical to an exact match when `year` is present (so pop/GDP
        # behaviour is unchanged), but also covers sparse proxies (e.g. a single degree-day year).
        lookup_year = min(available_years, key=lambda y: abs(int(y) - int(year)))
        idx = pd.MultiIndex.from_product([members, [lookup_year]], names=["iso2", "year"])
        series = frame.reindex(idx)["value"]
        missing = [
            c
            for c in series[series.isna()].index.get_level_values("iso2")
            if c in configured_countries
        ]
        if missing:
            raise ValueError(f"{name} proxy data missing for {missing} in year {lookup_year}.")
        series = series.fillna(0.0)
        series.index = series.index.get_level_values("iso2")
        term = weight * normalise(series)
        blended = term if blended is None else blended.add(term, fill_value=0.0)

    return normalise(blended).to_dict()

@deprecated(
    "build_ssp_shares is deprecated since 0.3.0; use build_proxy_shares with an explicit "
    "proxies registry instead."
)
def build_ssp_shares(
    members: list[str],
    year: int,
    sector: str,
    pop_data: pd.DataFrame,
    gdp_data: pd.DataFrame,
    sector_weights: dict,
    configured_countries: set[str] | None = None,
) -> dict[str, float]:
    """Deprecated thin shim over :func:`build_proxy_shares` for the population/GDP-only case.

    Prefer calling ``build_proxy_shares`` with an explicit ``proxies`` registry; this wrapper
    exists so pre-registry callers keep working.
    """
    return build_proxy_shares(
        members,
        year,
        sector,
        {"population": pop_data, "gdp": gdp_data},
        sector_weights,
        configured_countries,
    )
