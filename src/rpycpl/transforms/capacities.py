"""Build REMIND installed-capacity targets (p_nom_min) for PyPSA.

Ported from PyPSA-Eur ``import_REMIND_capacities.py``. The shared steps are unit conversion,
link output→input adjustment, and aggregation to PyPSA carriers. The REMIND-GDX-interface prep
(VRE-variant merge, battery scaling — which depend on REMIND tech names) is ``prepare_capacities``,
driven by the ``consolidation`` block of the ``capacity`` symbol spec, so it is strictly scoped to
the REMIND input (an IAMC/.mif config omits the block → no prep). ``build_capacity_targets`` chains
the whole recipe. Brownfield harmonisation / paid-off capacity is handled separately in
``rpycpl.capacities_etl`` (China path).
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


def prepare_capacities(
    caps: pd.DataFrame,
    *,
    vre_to_primary: dict[str, str] | None = None,
    battery_scaling: dict[str, float] | None = None,
    tech_col: str = "technology",
    value_col: str = "value",
) -> pd.DataFrame:
    """REMIND-GDX tech consolidation: merge VRE-coupled variants and fold battery storage into the
    battery-charger tech (``btin``).

    Both steps depend on REMIND tech names, so the params come from the ``consolidation`` block of
    the ``capacity`` symbol spec. With no params (e.g. an IAMC/.mif config that omits the block)
    this is a no-op. The ``btin`` guard makes it future-proof for bidirectional coupling: if the
    input already carries a ``btin`` capacity, it is used directly and the ``storX`` rows are
    dropped instead of being scaled.
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


def build_capacity_targets(
    loader,
    symbols: dict,
    regions: Sequence[str],
    tech_map: pd.DataFrame,
) -> pd.DataFrame:
    """Build REMIND installed-capacity floors (``p_nom_min``) per ``[year, region, carrier]``.

    Full recipe: read the ``capacity`` symbol (units applied by ``load_frame``), apply the
    REMIND-GDX ``consolidation`` declared on that symbol spec (VRE-variant merge, battery scaling,
    link-tech list), convert link-like techs to input-capacity basis, map REMIND techs to PyPSA
    carriers, and keep only ``regions``. When the spec has no ``consolidation`` block (e.g. an
    IAMC/.mif config) no scaling/merge/link-adjust is applied — strict REMIND-GDX scoping.
    """
    from rpycpl.io.remind_symbols import load_frame

    cap_spec = symbols["capacity"]
    cons = dict(cap_spec.get("consolidation", {}))
    link_techs = set(cons.pop("link_techs", []))

    caps = convert_capacities(load_frame(loader, cap_spec), unit_factor=1.0)
    caps = prepare_capacities(caps, **cons)

    if link_techs and "efficiency_conv" in symbols:
        eff = load_frame(loader, symbols["efficiency_conv"]).rename(columns={"value": "efficiency"})
        caps = adjust_link_capacities_to_input(caps, eff, link_techs)

    caps = aggregate_capacities_to_carriers(caps, tech_map)
    return caps[caps["region"].isin(set(regions))].reset_index(drop=True)
