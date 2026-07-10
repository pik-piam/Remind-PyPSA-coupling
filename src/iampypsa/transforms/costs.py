"""Shared, model-agnostic cost-override mechanics.

The *extraction* of individual cost parameters is source-specific and lives in each adapter.
What is genuinely shared — and lives here — is how a long-format cost-override table is mapped
to PyPSA carriers, basis-converted, given discount rates, and merged onto the PyPSA baseline
cost table; and how PyPSA-Eur baseline and fixed-value overrides are assembled from the
technology cost mapping CSV.

Unit factors are not defined here: they live centrally in ``iampypsa.units`` (re-exported below
for convenience) so any IAM adapter can swap the conversion table without touching transforms.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

import pandas as pd

# Unit conventions are centralized in iampypsa.units; re-exported here for convenient imports.
from iampypsa.units import DEFAULT_ETA_EXPONENTS

logger = logging.getLogger(__name__)


def broadcast_fuel_prices(
    df: pd.DataFrame,
    tech_fuel_map: Mapping[str, str] | None,
    *,
    tech_col: str = "technology",
    param_col: str = "parameter",
) -> pd.DataFrame:
    """Turn per-fuel ``fuel`` price rows into one ``fuel`` row per technology.

    Each technology in ``tech_fuel_map`` (canonical technology → fuel) gets a copy of its
    fuel's price row; technologies absent from the map get a synthesized ``fuel: 0`` row (they
    consume no priced primary-energy carrier). No-op when the map is absent.
    """
    if not tech_fuel_map:
        return df
    is_fuel = df[param_col] == "fuel"
    fuels = df[is_fuel]
    parts = [df[~is_fuel]]
    for tech, fuel in tech_fuel_map.items():
        rows = fuels[fuels[tech_col] == fuel]
        if rows.empty:
            logger.warning(
                "No '%s' fuel-price rows to broadcast to technology '%s'.", fuel, tech
            )
            continue
        rows = rows.copy()
        rows[tech_col] = tech
        parts.append(rows)

    modeled = df.loc[~is_fuel, ["region", tech_col]].drop_duplicates()
    no_fuel = modeled[~modeled[tech_col].isin(tech_fuel_map)].copy()
    if not no_fuel.empty:
        no_fuel[param_col] = "fuel"
        no_fuel["value"] = 0.0
        no_fuel["unit"] = "USD/MWh_th"
        parts.append(no_fuel)

    return pd.concat(parts, ignore_index=True)


def convert_investment_to_input_capacity_basis(
    costs: pd.DataFrame,
    eta_exponents: Mapping[str, float] = DEFAULT_ETA_EXPONENTS,
) -> pd.DataFrame:
    """Convert per-output-kW investment to per-input-kW by multiplying by efficiency**exp.

    Some IAMs report investment per kW of output capacity; PyPSA needs per kW of input
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
    source: str = "IAM",
    reference: str = "p_r",
) -> pd.DataFrame:
    """Add a ``discount rate`` row for every technology that does not already have one.

    ``source``/``reference`` annotate provenance; pass them through from the adapter's config
    so the same transform serves any IAM without code edits.
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


def _entries_by_source(technologies: Mapping[str, Any]) -> pd.DataFrame:
    """Flatten a ``technologies`` map to long ``[technology, canonical, parameter, source]``.

    ``source`` holds the raw per-parameter spec: the strings ``"IAM"``/``"PyPSA"`` or a
    ``{value: ...}`` dict — resolved from each entry's ``source:``/``overrides:`` (or bare
    string) via ``build_technology_sources``. ``canonical`` resolves the entry's ``iam_name:`` key
    (defaults to the entry name) via ``iam_name``.
    """
    from iampypsa.io.tech_params import iam_name, build_technology_sources

    rows = [
        {
            "technology": tech,
            "canonical": iam_name(tech, spec),
            "parameter": param,
            "source_spec": src,
        }
        for tech, spec in technologies.items()
        for param, src in build_technology_sources(spec).items()
    ]
    return pd.DataFrame(rows, columns=["technology", "canonical", "parameter", "source_spec"])


def build_mapped_overrides(
    technologies: Mapping[str, Any],
    model_long: pd.DataFrame,
    *,
    out_source: str = "IAM",
) -> pd.DataFrame:
    """Build overrides for parameters sourced from the IAM (``IAM`` entries).

    Each ``IAM`` entry pulls its value from ``model_long`` via its ``iam_name:`` (defaulting
    to the entry name). Raises ``ValueError`` listing every ``(technology, parameter)`` with
    no matching data — a real gap should be declared ``PyPSA``/``{value: ...}``, not ``IAM``.
    """
    entries = _entries_by_source(technologies)
    mapped = entries[entries["source_spec"] == "IAM"][["technology", "canonical", "parameter"]]
    merged = mapped.merge(
        model_long.rename(columns={"technology": "canonical"}),
        on=["canonical", "parameter"],
        how="left",
    )
    missing = merged[merged["value"].isna()]
    if not missing.empty:
        pairs = "\n".join(
            f"  {row.technology!r} (canonical {row.canonical!r}), parameter {row.parameter!r}"
            for row in missing.itertuples()
        )
        raise ValueError(
            f"'IAM' declared with no matching data in the adapter output:\n{pairs}\n"
            "Declare these 'PyPSA' or {value: ...} instead, or fix the IAM output."
        )
    overrides = merged[["region", "technology", "parameter", "value", "unit"]].copy()
    dups = overrides.duplicated(subset=["region", "technology", "parameter"], keep=False)
    if dups.any():
        raise ValueError(f"Duplicate (region, technology, parameter) after merge:\n{overrides[dups]}")
    overrides["source"] = out_source
    overrides["further description"] = f"Extracted from {out_source} model output"
    return overrides


def build_baseline_overrides(
    technologies: Mapping[str, Any],
    baseline_raw: pd.DataFrame,
    *,
    baseline_label: str = "PyPSA",
) -> pd.DataFrame:
    """Pull values from the model's baseline cost table for ``PyPSA`` entries.

    Silently drops ``(technology, parameter)`` pairs absent from ``baseline_raw`` — a
    structurally-expected gap (e.g. no ``fuel`` cost for storage) filled later via
    ``prepare_costs``'s ``fill_values``, not a real override.
    """
    entries = _entries_by_source(technologies)
    df = entries[entries["source_spec"] == "PyPSA"][["technology", "parameter"]]
    df = df.merge(
        baseline_raw,
        on=["technology", "parameter"],
        how="left",
        validate="one_to_one",
    )
    df = df.dropna(subset=["value"])
    df["source"] = baseline_label
    df["further description"] = f"Default parameter from {baseline_label} baseline cost file"
    return df[["technology", "parameter", "value", "unit", "source", "further description"]]


def build_set_value_overrides(
    technologies: Mapping[str, Any],
    origin: str,
) -> pd.DataFrame:
    """Return overrides for ``{value: <number>, unit: ..., comment: ...}`` entries."""
    entries = _entries_by_source(technologies)
    fixed = entries[entries["source_spec"].map(lambda s: isinstance(s, Mapping) and "value" in s)]
    set_df = fixed[["technology", "parameter"]].copy()
    set_df["value"] = pd.to_numeric(
        fixed["source_spec"].map(lambda s: s["value"]).values, errors="raise"
    )
    set_df["unit"] = fixed["source_spec"].map(lambda s: s.get("unit", "")).values
    set_df["further description"] = fixed["source_spec"].map(lambda s: s.get("comment", "")).values
    set_df["source"] = f"Set via configuration file: {origin}"
    return set_df[["technology", "parameter", "value", "unit", "further description", "source"]]


def apply_overrides(
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
