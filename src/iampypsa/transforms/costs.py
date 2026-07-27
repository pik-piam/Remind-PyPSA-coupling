"""Transform IAM techno-economic output into PyPSA cost tables.

Based on the technology mapping, extract cost parameters from the IAM output, from PyPSA costs
or directly set them from values specified in the mapping.

The *extraction* of individual cost parameters is source-specific and lives in each ``Coupler`` subclass.
The shared functions (which live here) — provide the conversion/mapping and merging tools.
"""

import logging
from collections.abc import Iterable, Mapping
from typing import Any

import pandas as pd

from iampypsa.io.technology_mapping import build_technology_sources, iam_name

logger = logging.getLogger(__name__)


def annotate_cost_rows(
    costs: pd.DataFrame,
    *,
    parameter: str,
    unit: str | None = None,
) -> pd.DataFrame:
    """Annotate ``parameter`` (and optionally ``unit``) onto a cost-row frame.

    Omit ``unit`` to keep the one the load layer already stamped from the spec's ``to_unit:``.
    Pass it only to override a declared unit — hardcoding a unit that merely restates the YAML
    lets the two drift apart.

    Args:
        costs: Cost rows to annotate.
        parameter: Canonical parameter name to write into the ``parameter`` column.
        unit: Unit to force onto the ``unit`` column, overriding the loaded one.
    """
    costs = costs.copy()
    costs["parameter"] = parameter
    if unit is not None:
        costs["unit"] = unit
    return costs


#: Currency-denominated parameters — the only rows ``apply_currency_factor`` scales.
#: Excludes physical units (lifetime/efficiency/CO2 intensity) and FOM (a ratio; factor cancels).
CURRENCY_PARAMETERS = {"investment", "VOM", "fuel"}


# TODO: Implement more generic deflator for different currency years
def apply_currency_factor(
    costs: pd.DataFrame,
    currency_factor: float,
    parameters: Iterable[str] = CURRENCY_PARAMETERS,
) -> pd.DataFrame:
    """Scale ``value`` by ``currency_factor`` for currency-denominated ``parameter`` rows.

    One-directional (IAM USD -> PyPSA baseline currency); converts between currencies, not
    between currency years (e.g. REMIND's US$2017 vs the baseline's own reporting year).
    """
    if currency_factor == 1.0:
        return costs
    costs = costs.copy()
    mask = costs["parameter"].isin(set(parameters))
    costs.loc[mask, "value"] *= currency_factor
    return costs


def broadcast_fuel_prices(
    costs: pd.DataFrame,
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
        return costs
    is_fuel = costs[param_col] == "fuel"
    fuels, rest = costs[is_fuel], costs[~is_fuel]

    mapping = pd.DataFrame(tech_fuel_map.items(), columns=[tech_col, "_fuel"])
    broadcast = mapping.merge(fuels.rename(columns={tech_col: "_fuel"}), on="_fuel").drop(columns="_fuel")
    missing = sorted(set(tech_fuel_map) - set(broadcast[tech_col]))
    if missing:
        logger.warning("No fuel-price rows to broadcast to technologies: %s", missing)

    modeled = rest[["region", tech_col]].drop_duplicates()
    no_fuel = modeled[~modeled[tech_col].isin(tech_fuel_map)].copy()
    no_fuel[param_col] = "fuel"
    no_fuel["value"] = 0.0
    no_fuel["unit"] = "USD/MWh_th"

    return pd.concat([rest, broadcast, no_fuel], ignore_index=True)


def convert_investment_to_input_capacity_basis(
    costs: pd.DataFrame,
    link_techs: Iterable[str],
) -> pd.DataFrame:
    """Convert per-output-kW investment to per-input-kW by multiplying by efficiency.

    IAMs report investment per kW of output capacity; PyPSA needs per kW of input (``p_nom``)
    for link-like technologies — ordinary generators need no adjustment and must not be listed
    in ``link_techs``. Which techs need this is specific to how the calling model's own
    network-building code consumes the result — see the caller for the reasoning per
    technology.
    """
    costs = costs.copy()
    for tech in link_techs:
        inv = (costs["technology"] == tech) & (costs["parameter"] == "investment")
        eff = (costs["technology"] == tech) & (costs["parameter"] == "efficiency")
        if inv.any() and eff.any():
            costs.loc[inv, "value"] *= costs.loc[eff, "value"].values
    return costs


def select_discount_rate(rates: pd.DataFrame, year: int, regions: Iterable[str]) -> pd.Series:
    """Return the discount rate per region for ``year``, indexed by region.

    Falls back to the most recent earlier year's value when a region has no value for
    ``year`` (e.g. a trailing NaN in the source), logging a warning for those regions.
    """
    last = rates.sort_values("year").groupby("region")["value"].last()
    missing = set(regions) - set(last.index)
    if missing:
        raise ValueError(
            f"No discount rate for year {year} or any earlier year, regions: {sorted(missing)}"
        )
    exact = rates[rates["year"].astype(str) == str(year)].set_index("region")["value"]
    filled = last.index.difference(exact.index)
    if len(filled):
        logger.warning(
            "Discount rate absent for year %d in regions %s; using most-recent value.",
            year, sorted(filled),
        )
    return exact.combine_first(last)


def add_discount_rate(
    costs: pd.DataFrame,
    discount_rate: float,
    *,
    source: str = "IAM",
    reference: str = "",
) -> pd.DataFrame:
    """Add a ``discount rate`` row for every technology that does not already have one.

    ``source``/``reference`` annotate provenance; pass them through from the Coupler's config
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


def _flatten_technology_mapping(technology_mapping: Mapping[str, Any]) -> pd.DataFrame:
    """Flatten a technology-mapping dict to long ``[technology, canonical, parameter, source_spec]``.

    ``source_spec`` holds the raw per-parameter spec: the strings ``"IAM"``/``"PyPSA"`` or a
    ``{value: ...}`` dict — resolved from each entry's ``source:``/``overrides:`` (or bare
    string) via ``build_technology_sources``. ``canonical`` resolves the entry's ``iam_name:`` key
    (defaults to the entry name) via ``iam_name``.

    Args:
        technology_mapping: The parsed technology-mapping YAML (``{"technologies": {...}}``'s
            inner dict — carrier name → spec), as loaded by ``load_technology_parameters``.
    """
    rows = [
        {
            "technology": tech,
            "canonical": iam_name(tech, spec),
            "parameter": param,
            "source_spec": src,
        }
        for tech, spec in technology_mapping.items()
        for param, src in build_technology_sources(spec).items()
    ]
    return pd.DataFrame(rows, columns=["technology", "canonical", "parameter", "source_spec"])


def build_iam_techdata(
    technology_mapping: Mapping[str, Any],
    iam_costs_long: pd.DataFrame,
    *,
    source: str = "IAM",
) -> pd.DataFrame:
    """Map ``IAM``-sourced parameter values onto target carriers, keeping the region dimension.

    Flattens ``technology_mapping`` (via ``build_technology_sources``), keeps the ``IAM`` entries,
    and merges each ``(canonical, parameter)`` against ``iam_costs_long``. Raises if an
    ``IAM``-declared entry has no matching data — those must be declared ``PyPSA`` or
    ``{value: ...}`` instead. Tags provenance columns consumed by ``apply_overrides``.

    Args:
        technology_mapping: The parsed technology-mapping YAML (carrier → spec).
        iam_costs_long: Long-format IAM cost table with columns
            ``region, technology, parameter, value, unit`` (e.g. a coupler's
            ``extract_cost_parameters`` output).
        source: Value to write into the output ``source`` column for provenance.

    Returns:
        Long frame ``[region, technology, parameter, value, unit, source, further description]``.
    """
    entries = _flatten_technology_mapping(technology_mapping)
    mapped = entries[entries["source_spec"] == "IAM"][["technology", "canonical", "parameter"]]
    merged = mapped.merge(
        iam_costs_long.rename(columns={"technology": "canonical"}),
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
            f"'IAM' declared with no matching data in the coupler output:\n{pairs}\n"
            "Declare these 'PyPSA' or {value: ...} instead, or fix the IAM output."
        )
    overrides = merged[["region", "technology", "parameter", "value", "unit"]].copy()
    dups = overrides.duplicated(subset=["region", "technology", "parameter"], keep=False)
    if dups.any():
        raise ValueError(f"Duplicate (region, technology, parameter) after merge:\n{overrides[dups]}")
    overrides["source"] = source
    overrides["further description"] = f"Extracted from {source} model output"
    return overrides


def build_pypsa_techdata(
    technology_mapping: Mapping[str, Any],
    pypsa_costs_long: pd.DataFrame,
    *,
    source: str = "PyPSA",
) -> pd.DataFrame:
    """Pull values from the model's own baseline cost table for ``PyPSA`` entries.

    Silently drops ``(technology, parameter)`` pairs absent from ``pypsa_costs_long`` — a
    structurally-expected gap (e.g. no ``fuel`` cost for storage) filled later via
    ``prepare_costs``'s ``fill_values``, not a real override.

    Args:
        technology_mapping: The parsed technology-mapping YAML (carrier → spec).
        pypsa_costs_long: The PyPSA model's own long-format baseline cost table, columns
            ``technology, parameter, value, unit`` (no region dimension — baseline costs are
            model-wide, not regional).
        source: Value to write into the output ``source`` column for provenance.

    Returns:
        Long frame ``[technology, parameter, value, unit, source, further description]``.
    """
    entries = _flatten_technology_mapping(technology_mapping)
    df = entries[entries["source_spec"] == "PyPSA"][["technology", "parameter"]]
    df = df.merge(
        pypsa_costs_long,
        on=["technology", "parameter"],
        how="left",
        validate="one_to_one",
    )
    df = df.dropna(subset=["value"])
    df["source"] = source
    df["further description"] = f"Default parameter from {source} baseline cost file"
    return df[["technology", "parameter", "value", "unit", "source", "further description"]]


def build_fixed_value_overrides(
    technology_mapping: Mapping[str, Any],
    *,
    source: str = "fixed",
) -> pd.DataFrame:
    """Return overrides for technology-mapping entries with a fixed-value spec.

    A "fixed-value" entry is one whose per-parameter source is a dict of the shape
    ``{value: <number>, unit: <str>, comment: <str>}``, setting a fixed value directly.
    ``unit`` and ``comment`` are optional and default to ``""``.

    Args:
        technology_mapping: The parsed technology-mapping YAML (carrier → spec).
        source: Value to write into the output ``source`` column for provenance.

    Returns:
        Long frame ``[technology, parameter, value, unit, further description, source]``.
    """
    entries = _flatten_technology_mapping(technology_mapping)
    fixed = entries[entries["source_spec"].map(lambda s: isinstance(s, Mapping) and "value" in s)]
    set_df = fixed[["technology", "parameter"]].copy()
    set_df["value"] = pd.to_numeric(
        fixed["source_spec"].map(lambda s: s["value"]).values, errors="raise"
    )
    set_df["unit"] = fixed["source_spec"].map(lambda s: s.get("unit", "")).values
    set_df["further description"] = fixed["source_spec"].map(lambda s: s.get("comment", "")).values
    set_df["source"] = source
    return set_df[["technology", "parameter", "value", "unit", "further description", "source"]]


def apply_overrides(
    costs: pd.DataFrame,
    overrides: pd.DataFrame,
) -> pd.DataFrame:
    """Update and insert ``overrides`` onto ``costs`` by ``(technology, parameter)``.

    Rows whose key is absent from ``costs`` are added; for keys present in both,
    ``value``/``unit``/``source``/``further description`` are overwritten from ``overrides``.
    """
    base = costs.set_index(["technology", "parameter"]).copy()
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
