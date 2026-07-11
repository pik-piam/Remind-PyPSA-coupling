"""Determine installed-capacity targets (p_nom_min) for PyPSA from IAM output.

The shared pipeline is: 
1. read the capacity spec via ``load_spec`` (handles unit conversion and
backend dispatch)
2. optionally apply a ``consolidation`` block from the spec (VRE-variant merging,
battery scaling — only exercised when the symbol config declares it, e.g. for GDX input),
3. adjust link-like techs to input-capacity basis, and aggregate to PyPSA carriers.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

import pandas as pd

from iampypsa.io.remind_symbols import load_spec, rename_technologies

logger = logging.getLogger(__name__)


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
    (PyPSA links are defined by input capacity, but some IAMs report output capacity.)

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
    map_tech_col: str,
    map_carrier_col: str,
    value_col: str = "value",
    unit: str = "MW",
    min_value: float = 0.0,
    round_digits: int = 2,
) -> pd.DataFrame:
    """Map model tech tokens to target carrier names, sum per (group, carrier).

    Returns ``[year, region, carrier, value, unit]``.
    Rows whose tech token is absent from ``tech_to_carrier`` are dropped with a warning.
    Aggregation is needed when the mapping is not 1:1 (several tokens share a carrier).
    """
    carrier_map = tech_to_carrier[[map_tech_col, map_carrier_col]].drop_duplicates(
        subset=map_tech_col, keep="first"
    )
    mapped = capacities.merge(carrier_map, left_on=tech_col, right_on=map_tech_col, how="left")
    unmapped = mapped[map_carrier_col].isna().sum()
    if unmapped:
        logger.warning("Dropping %d rows with unmapped technologies.", int(unmapped))
    mapped = mapped.dropna(subset=[map_carrier_col]).rename(columns={map_carrier_col: "carrier"})

    grouped = (
        mapped.groupby([*group_cols, "carrier"], as_index=False, observed=True)[value_col]
        .sum()
        .round(round_digits)
    )
    grouped = grouped[grouped[value_col] > min_value]
    grouped["unit"] = unit
    return grouped.sort_values([*group_cols, "carrier"]).reset_index(drop=True)


def apply_consolidation(
    caps: pd.DataFrame,
    *,
    vre_to_primary: dict[str, str] | None = None,
    battery_scaling: dict[str, float] | None = None,
    tech_col: str = "technology",
    value_col: str = "value",
) -> pd.DataFrame:
    """Apply the optional ``consolidation`` block from the capacity symbol spec.

    Two steps, both driven by config — no-op when params are absent (e.g. IAMC configs
    that have no ``consolidation`` block):

    1. **VRE-variant merge**: rename coupled VRE tech tokens to their primary token
       (e.g. ``elh2VRE`` → ``elh2``).
    2. **Battery scaling**: fold storage tech rows into the charger token (``btin``) by
       multiplying each storage row by its scaling factor. If a ``btin`` row already carries
       a positive value, the storage rows are dropped instead (bidirectional-coupling guard).
    """
    caps = caps.copy()
    vre_to_primary = vre_to_primary or {}
    battery_scaling = battery_scaling or {}

    tech = caps[tech_col].astype(str)
    caps[tech_col] = tech.map(lambda t: vre_to_primary.get(t, t))

    if not battery_scaling:
        return caps

    tech = caps[tech_col].astype(str)
    is_btin_present = ((tech == "btin") & (caps[value_col] > 0)).any()
    is_stor = tech.isin(battery_scaling)
    if is_btin_present:
        return caps[~is_stor].copy()
    scale = tech.map(battery_scaling)
    caps.loc[scale.notna(), value_col] *= scale[scale.notna()]
    caps[tech_col] = tech.map(lambda t: "btin" if t in battery_scaling else t)
    return caps


def prepare_capacities(loader, symbols: dict) -> pd.DataFrame:
    """Read capacities at model-tech resolution, before any carrier aggregation.

    Shared preparation for every consumer:
    1. Read the ``capacity`` spec via ``load_spec`` (dispatches on spec shape; unit conversion
       applied by the spec's ``to_unit``).
    2. Apply the ``consolidation`` block if present (VRE-variant merge, battery scaling).
    3. Adjust link-like techs to input-capacity basis (requires ``efficiency_conv`` symbol).

    Returns ``[year, region, technology, value, unit]``. Callers that need PyPSA carriers pass
    this to :func:`aggregate_capacities_to_carriers`; callers that need model-tech resolution
    (e.g. group-wise brownfield harmonisation) consume it directly.
    """
    cap_spec = symbols["capacity"]
    cons = dict(cap_spec.get("consolidation", {}))
    link_techs = set(cons.pop("link_techs", []))

    caps = load_spec(loader, cap_spec)
    caps = apply_consolidation(caps, **cons)

    if link_techs and "efficiency_conv" in symbols:
        eff = load_spec(loader, symbols["efficiency_conv"]).rename(columns={"value": "efficiency"})
        caps = adjust_link_capacities_to_input(caps, eff, link_techs)

    return caps


def build_capacity_targets(
    loader,
    symbols: dict,
    regions: Sequence[str],
    tech_map: pd.DataFrame,
    *,
    map_tech_col: str,
    map_carrier_col: str,
) -> pd.DataFrame:
    """Build installed-capacity targets per ``[year, region, carrier, value, unit]``.

    Prepare capacities (:func:`prepare_capacities`), map tech tokens to carriers via ``tech_map``
    and sum, then filter to ``regions``. The ``unit`` column reflects the target unit declared in
    the capacity spec (``to_unit``).
    """
    unit = symbols["capacity"].get("to_unit", "MW")

    caps = prepare_capacities(loader, symbols)
    caps = rename_technologies(caps, symbols.get("technology_names"))
    caps = aggregate_capacities_to_carriers(
        caps, tech_map, map_tech_col=map_tech_col, map_carrier_col=map_carrier_col, unit=unit,
    )
    caps["year"] = caps["year"].astype(int)
    return caps[caps["region"].isin(set(regions))].reset_index(drop=True)
