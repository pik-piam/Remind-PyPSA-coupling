"""Read REMIND/IAM output from the IAMC ``.mif`` exchange format.

An ``.mif`` is a ``;``-separated table with five id columns
``Model;Scenario;Region;Variable;Unit`` followed by one column per year; rows are one
variable × region combination. A trailing ``;`` in each line produces a spurious unnamed
column, which is dropped on read. All five id columns are lower-cased on import so callers
reference them uniformly as ``model``, ``scenario``, ``region``, ``variable``, ``unit``.

``assemble_variable_set`` is the generic layer between the IAMC long frame and the
token-labelled frames the coupling adapters consume. It knows nothing about REMIND — it
receives a ``mapping`` dict (variable → token label) and an optional ``derived`` dict for
linear combinations (e.g. ``pc = Coal|w/o CC − IGCC − CHP``).
"""

from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from os import PathLike

import pandas as pd

from iampypsa.units import unit_factor

logger = logging.getLogger(__name__)

ID_COLUMNS = ["model", "scenario", "region", "variable", "unit"]

# IAMC id-column header names (as written in the file, before lower-casing).
_ID_RAW = ["Model", "Scenario", "Region", "Variable", "Unit"]

# REMIND writes variable trees with "+"-only segments marking the current summation level
# (e.g. ``Cap|Electricity|+|Nuclear``, ``Cap|Electricity|Gas|CC|+|w/o CC``). Those markers are a
# pure reporting artifact — the canonical variable name has no "+" — and their placement varies
# with reporting depth. Strip them so symbol maps reference clean names (matching the wider REMIND
# tooling, e.g. piamInterfaces/remind2). Verified collision-free on the reference REMIND .mif.
_AGG_MARKER = re.compile(r"\|\++\|")


def _strip_agg_markers(variables: "pd.Series") -> "pd.Series":
    """Remove REMIND ``|+|``/``|++|`` summation-level markers from IAMC variable names."""
    return variables.str.replace(_AGG_MARKER, "|", regex=True)


def read_iamc(
    path: str | PathLike,
    variables: Sequence[str] | None = None,
    sep: str = ";",
) -> pd.DataFrame:
    """Read an IAMC ``.mif`` file into a tidy long DataFrame.

    Returns columns ``[model, scenario, region, variable, unit, year, value]``; ``NA``
    entries and rows with ``NaN`` value are dropped. Pass ``variables`` to filter early
    (before the expensive melt) on the ~167k-row files typical of REMIND output.
    """
    raw = pd.read_csv(path, sep=sep, na_values=["NA"], dtype=str)
    # Drop trailing unnamed column produced by a trailing semicolon on every data row.
    raw = raw.loc[:, ~raw.columns.str.startswith("Unnamed")]
    # Lower-case the five id columns so callers can rely on consistent names.
    rename = {c: c.lower() for c in _ID_RAW if c in raw.columns}
    raw = raw.rename(columns=rename)
    # Canonicalise variable names by dropping REMIND summation-level "+" markers (see above),
    # so both the early filter and downstream mapping match clean spec names.
    if "variable" in raw.columns:
        raw["variable"] = _strip_agg_markers(raw["variable"])
    # Filter before the expensive melt — on a 167k-variable file this matters.
    if variables is not None:
        raw = raw[raw["variable"].isin(set(variables))]
    # Every non-id column is a year.
    id_present = [c for c in ID_COLUMNS if c in raw.columns]
    year_cols = [c for c in raw.columns if c not in ID_COLUMNS]
    long = raw.melt(id_vars=id_present, value_vars=year_cols, var_name="year", value_name="value")
    long["year"] = long["year"].astype(int)
    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    return long.dropna(subset=["value"]).reset_index(drop=True)


def list_iamc_variables(path: str | PathLike, sep: str = ";") -> list[str]:
    """List the IAMC variable names present in a ``.mif`` file (sorted)."""
    raw = pd.read_csv(path, sep=sep, usecols=["Variable"], dtype=str)
    return sorted(_strip_agg_markers(raw["Variable"].dropna()).unique().tolist())


def parse_currency_year(unit: str) -> int | None:
    """Extract the reference year from a unit string such as ``'US$2017/kW'`` → 2017.

    Building block for future automatic currency-year handling (deflating IAMC cost units to
    a common reference year). Not yet wired into the cost pipeline.
    """
    m = re.search(r"US\$(\d{4})", unit)
    return int(m.group(1)) if m else None


def assemble_variable_set(
    df: pd.DataFrame,
    mapping: dict[str, str],
    *,
    label_col: str = "technology",
    derived: dict[str, list[tuple[float, str]]] | None = None,
    to_unit: str | None = None,
) -> pd.DataFrame:
    """Map IAMC variables to token labels and compute derived linear combinations.

    Generic — no REMIND-specific knowledge. Receives a long IAMC frame (from
    ``read_iamc``) plus caller-supplied mappings, returns
    ``[year, region, <label_col>, value, unit]``.

    Parameters
    ----------
    df:
        Long IAMC frame with ``[region, variable, unit, year, value]`` columns.
    mapping:
        ``{variable_name: token_label}`` — direct one-to-one assignments.
    label_col:
        Column name for the output token column (``"technology"`` by default).
    derived:
        ``{token: [(coefficient, variable_name), ...]}`` — linear combinations built from
        variables in ``df`` (which may also appear in ``mapping``). Missing component
        variables propagate NaN (the row is dropped).
    to_unit:
        Target unit string.  Source unit is read homogeneously from the ``unit`` column of
        ``df``; ``unit_factor(src, to_unit)`` is applied.  Pass ``None`` to keep the
        source unit unchanged.

    Returns
    -------
    pd.DataFrame
        ``[year, region, <label_col>, value, unit]``, sorted by year/region/token.
    """
    all_vars: set[str] = set(mapping) | {v for terms in (derived or {}).values() for _, v in terms}
    sub = df[df["variable"].isin(all_vars)].copy()

    # Determine source unit — must be homogeneous across the whole variable set.
    units_present = sub["unit"].dropna().unique()
    if len(units_present) == 0:
        src_unit = None
    elif len(units_present) == 1:
        src_unit = units_present[0]
    else:
        raise ValueError(
            f"Heterogeneous units in IAMC variable set: {sorted(units_present)}. "
            "Split into separate specs or use different to_unit per parameter."
        )

    scale = unit_factor(src_unit, to_unit) if (to_unit and src_unit and src_unit != to_unit) else 1.0
    out_unit = to_unit if to_unit is not None else src_unit

    group_cols = ["year", "region"]
    frames: list[pd.DataFrame] = []

    # --- direct variable → token assignments ---
    direct = sub[sub["variable"].isin(mapping)].copy()
    if not direct.empty:
        direct[label_col] = direct["variable"].map(mapping)
        direct["value"] = direct["value"] * scale
        direct["unit"] = out_unit
        frames.append(direct[group_cols + [label_col, "value", "unit"]])

    # --- derived: linear combinations ---
    if derived:
        wide = sub.pivot_table(
            index=group_cols, columns="variable", values="value", aggfunc="sum"
        )
        for token, terms in derived.items():
            vals: pd.Series | None = None
            for coeff, var in terms:
                col = wide.get(var)  # None if variable absent entirely
                if col is None:
                    logger.warning(
                        "Derived token %r: component variable %r absent from mif; "
                        "skipping this token.",
                        token, var,
                    )
                    vals = None
                    break
                vals = (vals + coeff * col) if vals is not None else coeff * col
            if vals is not None:
                token_df = vals.reset_index()
                token_df.columns = [*group_cols, "value"]
                token_df[label_col] = token
                token_df["value"] = token_df["value"] * scale
                token_df["unit"] = out_unit
                frames.append(
                    token_df[group_cols + [label_col, "value", "unit"]].dropna(subset=["value"])
                )

    if not frames:
        return pd.DataFrame(columns=group_cols + [label_col, "value", "unit"])

    result = pd.concat(frames, ignore_index=True)
    return result.sort_values(group_cols + [label_col]).reset_index(drop=True)
