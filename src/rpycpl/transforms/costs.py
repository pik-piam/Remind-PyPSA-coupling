"""Shared, model-agnostic cost-override mechanics.

The *extraction* of individual REMIND cost parameters is source-specific and lives in each
adapter. What is genuinely shared — and lives here — is how a long-format cost-override table
is mapped to PyPSA carriers, basis-converted, given discount rates, and merged onto the PyPSA
baseline cost table; and how PyPSA-Eur baseline and fixed-value overrides are assembled from
the technology cost mapping CSV.

Unit factors are not defined here: they live centrally in ``rpycpl.units`` (re-exported below
for convenience) so a non-REMIND IAM can swap the conversion table without touching transforms.
"""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd

# Unit conventions are centralized in rpycpl.units; re-exported here for convenient imports.
from rpycpl.units import DEFAULT_ETA_EXPONENTS


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


def build_mapped_overrides(
    technology_mapping: pd.DataFrame,
    model_long: pd.DataFrame,
    *,
    tech_col: str,
    ref_col: str,
    param_col: str,
    source_col: str,
    model_value: str,
    out_source: str,
) -> pd.DataFrame:
    """Map model parameter values onto target carriers and log missing references.

    Wraps ``build_cost_overrides`` with a warning for each mapped (reference, parameter) pair
    that is absent from ``model_long`` — those fall back to the baseline on merge.
    Tags provenance columns consumed by ``apply_overrides``.
    """
    import logging
    logger = logging.getLogger(__name__)

    mapped = technology_mapping.loc[
        technology_mapping[source_col] == model_value, [tech_col, ref_col, param_col]
    ]
    merged = mapped.merge(model_long, on=[ref_col, param_col], how="left")
    merged = merged[~merged["value"].isna()]
    overrides = merged.rename(columns={tech_col: "technology"})[
        ["region", "technology", param_col, "value", "unit"]
    ].copy()
    dups = overrides.duplicated(subset=["region", "technology", param_col], keep=False)
    if dups.any():
        raise ValueError(f"Duplicate (region, technology, parameter) after merge:\n{overrides[dups]}")

    present = set(zip(model_long[ref_col], model_long[param_col]))
    for _, row in technology_mapping[technology_mapping[source_col] == model_value].iterrows():
        if (row[ref_col], row[param_col]) not in present:
            logger.warning(
                "Reference '%s' (→ '%s', parameter '%s') absent from model output"
                " — falling back to baseline.",
                row[ref_col], row[tech_col], row[param_col],
            )
    overrides[source_col] = out_source
    overrides["further description"] = f"Extracted from {out_source} model output"
    return overrides


def build_baseline_overrides(
    technology_mapping: pd.DataFrame,
    baseline_raw: pd.DataFrame,
    *,
    tech_col: str,
    source_col: str,
    baseline_value: str,
) -> pd.DataFrame:
    """Pull parameter values from the baseline cost table for rows marked source=<baseline_value>."""
    df = technology_mapping[technology_mapping[source_col] == baseline_value].drop(columns=["unit"])
    df = df.merge(
        baseline_raw,
        left_on=[tech_col, "parameter"],
        right_on=["technology", "parameter"],
        how="left",
        validate="one_to_one",
    )
    df[source_col] = baseline_value
    df["further description"] = f"Default parameter from {baseline_value} baseline cost file"
    return df[["technology", "parameter", "value", "unit", source_col, "further description"]]


def build_set_value_overrides(
    technology_mapping: pd.DataFrame,
    mapping_file: str,
    *,
    tech_col: str,
    source_col: str,
    fixed_value: str,
    comment_col: str,
) -> pd.DataFrame:
    """Return overrides for rows marked source=<fixed_value>, with reference parsed as a number."""
    set_df = (
        technology_mapping[technology_mapping[source_col] == fixed_value]
        .rename(columns={
            tech_col: "technology",
            "reference": "value",
            comment_col: "further description",
        })[["technology", "parameter", "value", "unit", "further description"]]
        .copy()
    )
    set_df["value"] = pd.to_numeric(set_df["value"], errors="raise")
    set_df[source_col] = f"Set via configuration file: {mapping_file}"
    set_df["further description"] = set_df["further description"].fillna("")
    return set_df


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
