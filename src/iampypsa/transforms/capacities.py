"""Pure steps for turning IAM installed capacities into PyPSA targets (p_nom_min).

In pipeline order: postprocess variant tokens (:func:`apply_postprocessing`), put link-like
technologies on an input-capacity basis (:func:`adjust_link_capacities_to_input`), then map
model tech tokens to PyPSA carriers and sum (:func:`aggregate_capacities_to_carriers`).

Reading the symbols and sequencing these is the Coupler's job — see
``Coupler.prepare_capacities`` / ``Coupler.get_capacities``.
"""

import logging
from collections.abc import Sequence

import pandas as pd

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
    (PyPSA links are defined by input capacity, but IAMs report output capacity.)

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

    Returns ``[year, region, carrier, value, unit]``. ``tech_to_carrier`` may be many-to-many:
    several tokens sharing a carrier are summed; one token feeding several carriers is
    preserved (not deduped), each carrier getting the full value. Rows whose tech token is
    absent from ``tech_to_carrier`` are dropped with a warning.
    """
    carrier_map = tech_to_carrier[[map_tech_col, map_carrier_col]].drop_duplicates()
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


# TODO symbol still referenced
def apply_postprocessing(
    caps: pd.DataFrame,
    *,
    merge: dict[str, list[str]] | None = None,
    scale: dict[str, float] | None = None,
    tech_col: str = "technology",
    value_col: str = "value",
) -> pd.DataFrame:
    """Apply the optional ``postprocessing`` block from the capacity symbol spec.

    Two steps, both driven by config — no-op when params are absent (e.g. IAMC configs
    that have no ``postprocessing`` block):

    1. **Merge**: rename coupled variant tokens to their primary token via ``merge``, a
       ``{target: [sources, ...]}`` map (e.g. ``{"elh2": ["elh2", "elh2VRE"]}`` merges
       ``elh2VRE`` into ``elh2``; also used for scaling targets below, e.g.
       ``{"btin": ["btin", "storspv", ...]}``).
    2. **Scale**: multiply each ``scale`` source row by its scaling factor before it's merged
       into its ``merge`` target. If that target already carries a positive value on its own,
       the source rows are dropped instead of scaled (bidirectional-coupling guard).
    """
    caps = caps.copy()
    merge = merge or {}
    scale = scale or {}
    rename_map = {source: target for target, sources in merge.items() for source in sources}

    tech = caps[tech_col].astype(str)

    if scale:
        targets = {source: rename_map.get(source, source) for source in scale}
        is_target_present = tech.isin(set(targets.values())) & (caps[value_col] > 0)
        is_scaled = tech.isin(scale)
        if is_target_present.any():
            caps = caps[~is_scaled].copy()
            tech = caps[tech_col].astype(str)
        else:
            factor = tech.map(scale)
            caps.loc[factor.notna(), value_col] *= factor[factor.notna()]

    caps[tech_col] = tech.map(lambda t: rename_map.get(t, t))
    return caps


