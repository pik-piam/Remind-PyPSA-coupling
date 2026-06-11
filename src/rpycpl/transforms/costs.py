"""Shared, model-agnostic cost-override mechanics.

The *extraction* of individual REMIND cost parameters (which GDX symbols, which unit
factors) is REMIND-interface/model-specific and lives in each model's adapter (it differs
between the EUR coupling GDX and China's raw fulldata GDX). What is genuinely shared — and
lives here — is how a long-format cost-override table is mapped to PyPSA carriers,
basis-converted, given discount rates, and merged onto the PyPSA baseline cost table.

Unit factors are not defined here: they live centrally in ``rpycpl.units`` (re-exported below
for convenience) so a non-REMIND IAM can swap the conversion table without touching transforms.
"""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd

# Unit conventions are centralized in rpycpl.units; re-exported here for convenient imports.
from rpycpl.units import DEFAULT_ETA_EXPONENTS


def build_cost_overrides(
    tech_map: pd.DataFrame,
    remind_long: pd.DataFrame,
    *,
    tech_col: str = "PyPSA-Eur technology",
    ref_col: str = "reference",
    param_col: str = "parameter",
    source_col: str = "source",
    remind_value: str = "REMIND",
) -> pd.DataFrame:
    """Build the long cost-override table by mapping REMIND values onto PyPSA carriers.

    One row per (region, technology, parameter) via a 1:1 tech-map lookup; rows whose REMIND
    reference is absent from ``remind_long`` are dropped (the baseline value is kept on merge).
    Raises on duplicate (region, technology, parameter).
    """
    # Keep only the keys + target carrier from the map so its other columns (e.g. a 'unit'
    # column) don't collide with the REMIND frame's columns on merge.
    mapped = tech_map.loc[tech_map[source_col] == remind_value, [tech_col, ref_col, param_col]]
    merged = mapped.merge(remind_long, on=[ref_col, param_col], how="left")
    merged = merged[~merged["value"].isna()]
    out = merged.rename(columns={tech_col: "technology"})[
        ["region", "technology", param_col, "value", "unit"]
    ].copy()
    dups = out.duplicated(subset=["region", "technology", param_col], keep=False)
    if dups.any():
        raise ValueError(f"Duplicate (region, technology, parameter) after merge:\n{out[dups]}")
    return out


def convert_investment_to_input_capacity_basis(
    costs: pd.DataFrame,
    eta_exponents: Mapping[str, float] = DEFAULT_ETA_EXPONENTS,
) -> pd.DataFrame:
    """Convert per-output-kW investment to per-input-kW by multiplying by efficiency**exp.

    REMIND reports investment per kW of output capacity; PyPSA needs per kW of input
    (``p_nom``). For each technology in ``eta_exponents``, ``investment`` is multiplied by
    ``efficiency ** exp`` (exp=1 uses eta directly; exp=0.5 takes the one-way value out of a
    pre-squared round-trip efficiency).
    """
    costs = costs.copy()
    for tech, exp in eta_exponents.items():
        inv = (costs["technology"] == tech) & (costs["parameter"] == "investment")
        eff = (costs["technology"] == tech) & (costs["parameter"] == "efficiency")
        if inv.any() and eff.any():
            costs.loc[inv, "value"] *= costs.loc[eff, "value"].values ** exp
    return costs


def add_discount_rate(
    costs: pd.DataFrame,
    discount_rate: float,
    *,
    source: str = "REMIND",
    reference: str = "p_r",
) -> pd.DataFrame:
    """Add a ``discount rate`` row for every technology that does not already have one.

    ``source``/``reference`` annotate provenance and default to REMIND's; pass them through
    (from the adapter's config) so the same transform serves another IAM without code edits.
    """
    have = costs.loc[costs["parameter"] == "discount rate", "technology"]
    missing = costs.loc[~costs["technology"].isin(have), ["technology"]].drop_duplicates()
    dr = pd.DataFrame(
        {
            "parameter": ["discount rate"],
            "value": [discount_rate],
            "unit": ["p.u."],
            "source": [source],
            "further description": [reference],
        }
    ).merge(missing, how="cross")
    return pd.concat([costs, dr], ignore_index=True)


def merge_cost_overrides_into_baseline(
    baseline_raw: pd.DataFrame,
    overrides: pd.DataFrame,
) -> pd.DataFrame:
    """Apply overrides onto the baseline cost table, adding new (technology, parameter) rows."""
    base = baseline_raw.set_index(["technology", "parameter"]).copy()
    ov = overrides.set_index(["technology", "parameter"]).copy()
    if ov.index.duplicated().any():
        raise ValueError(
            f"Duplicate overrides for (technology, parameter): "
            f"{ov.index[ov.index.duplicated()].tolist()}"
        )
    extra = ov.index.difference(base.index)
    if len(extra) > 0:
        base = pd.concat([base, ov.loc[extra, base.columns.intersection(ov.columns)]])
    shared = ov.index.intersection(base.index)
    for col in ["value", "unit", "source", "further description"]:
        if col in ov.columns:
            base.loc[shared, col] = ov.loc[shared, col]
    merged = base.reset_index()
    if merged.duplicated(subset=["technology", "parameter"]).any():
        dups = merged[merged.duplicated(subset=["technology", "parameter"], keep=False)]
        raise ValueError(f"Duplicates after merge: {dups}")
    return merged
