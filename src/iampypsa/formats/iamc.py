"""Read IAM output from the IAMC ``.mif`` exchange format.

An ``.mif`` is a ``;``-separated table with five id columns
``Model;Scenario;Region;Variable;Unit`` followed by one column per year; rows are one
variable × region combination. A trailing ``;`` in each line produces a spurious unnamed
column, which is dropped on read. All five id columns are lower-cased on import so callers
reference them uniformly as ``model``, ``scenario``, ``region``, ``variable``, ``unit``.

``build_variable_set`` is the layer between the IAMC long frame and the token-labelled frames
the ``Coupler`` classes consume: it receives a ``mapping`` dict (variable → token label) and an
optional ``derived`` dict for linear combinations (e.g. ``pc = Coal|w/o CC − IGCC − CHP``).
``load_variables`` drives it from a ``variables:`` quantity spec.
"""

import functools
import logging
import re
from collections.abc import Sequence
from os import PathLike

import pandas as pd

from iampypsa.units import unit_factor

logger = logging.getLogger(__name__)

#: Backends whose data follows this model.
BACKENDS = frozenset({"iamc"})

ID_COLUMNS = ["model", "scenario", "region", "variable", "unit"]

# IAMC id-column header names (as written in the file, before lower-casing).
_ID_RAW = ["Model", "Scenario", "Region", "Variable", "Unit"]

# IAMC variable trees carry "+"-only reporting-depth markers (e.g. ``Cap|Electricity|+|Nuclear``)
# that aren't part of the canonical name and whose placement varies by depth.
_AGG_MARKER = re.compile(r"\|\++\|")


def strip_agg_markers(variables: pd.Series) -> pd.Series:
    """Remove ``|+|``/``|++|`` summation-level markers from IAMC variable names."""
    return variables.str.replace(_AGG_MARKER, "|", regex=True)


def read_iamc(
    path: str | PathLike,
    variables: Sequence[str] | None = None,
    sep: str = ";",
    agg_markers: bool = True,
) -> pd.DataFrame:
    """Read an IAMC ``.mif`` file into a long DataFrame.

    Returns columns ``[model, scenario, region, variable, unit, year, value]``; ``NA``
    entries and rows with ``NaN`` value are dropped. Pass ``variables`` to filter early.

    ``agg_markers`` strips reporting-depth markers so specs can name clean variables. It
    defaults on because the markers are reserved by the IAMC convention, so stripping is a
    no-op for files that don't use them.
    """
    raw = pd.read_csv(path, sep=sep, na_values=["NA"], dtype=str)
    # Drop trailing unnamed column produced by a trailing semicolon on every data row.
    raw = raw.loc[:, ~raw.columns.str.startswith("Unnamed")]
    rename = {c: c.lower() for c in _ID_RAW if c in raw.columns}
    raw = raw.rename(columns=rename)
    if agg_markers and "variable" in raw.columns:
        raw["variable"] = strip_agg_markers(raw["variable"])
    # Filter before the expensive melt — on a 167k-variable file this matters.
    if variables is not None:
        raw = raw[raw["variable"].isin(set(variables))]
    id_present = [c for c in ID_COLUMNS if c in raw.columns]
    year_cols = [c for c in raw.columns if c not in ID_COLUMNS]
    long = raw.melt(id_vars=id_present, value_vars=year_cols, var_name="year", value_name="value")
    long["year"] = long["year"].astype(int)
    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    return long.dropna(subset=["value"]).reset_index(drop=True)


@functools.lru_cache(maxsize=8)
def _read_iamc_variables(path: str, sep: str) -> tuple[str, ...]:
    """Read and cache the sorted variable names of one ``.mif`` (keyed by path, like the GDX
    container cache). Every symbol resolution scans this column, so on a full-size mif the
    repeated reads dominate."""
    raw = pd.read_csv(path, sep=sep, usecols=["Variable"], dtype=str)
    return tuple(sorted(strip_agg_markers(raw["Variable"].dropna()).unique().tolist()))


def list_iamc_variables(path: str | PathLike, sep: str = ";") -> list[str]:
    """List the IAMC variable names present in a ``.mif`` file (sorted)."""
    return list(_read_iamc_variables(str(path), sep))


def parse_currency_year(unit: str) -> int | None:
    """Extract the reference year from a unit string such as ``'US$2017/kW'`` → 2017.

    Building block for future automatic currency-year handling (deflating IAMC cost units to
    a common reference year). Not yet wired into the cost pipeline.
    """
    m = re.search(r"US\$(\d{4})", unit)
    return int(m.group(1)) if m else None


def build_variable_set(
    df: pd.DataFrame,
    mapping: dict[str, str],
    *,
    label_col: str = "technology",
    derived: dict[str, list[tuple[float, str]]] | None = None,
    to_unit: str | None = None,
) -> pd.DataFrame:
    """Map IAMC variables to token labels and compute derived linear combinations.

    Model-agnostic. Receives a long IAMC frame (from
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


def load_variables(loader, spec: dict[str, str]) -> pd.DataFrame:
    """Load a ``variables:`` spec: many IAMC variables → one token-labelled frame.

    ``variables:`` maps IAMC variable names to token labels; optional ``derived:`` declares
    linear combinations. Fallback tokens in ``spec['fallback']`` (``{token: {value, unit,
    reason}}``) are synthesised for every ``(year, region)`` when absent from the data.

    Reads ``loader.source`` directly: "one table filtered to N variables" is IAMC's own
    container semantics, which this module owns. Only the *source* comes from the loader.
    """
    mapping: dict[str, str] = spec["variables"]
    derived: dict[str, list] | None = spec.get("derived")
    label_col: str = spec.get("label_col", "technology")
    to_unit: str | None = spec.get("to_unit")

    # Collect all variable names (direct + derived components).
    direct_vars = set(mapping)
    derived_vars: set[str] = set()
    if derived:
        for terms in derived.values():
            derived_vars.update(v for _, v in terms)
    all_vars = list(direct_vars | derived_vars)

    df = read_iamc(loader.source, variables=all_vars)
    result = build_variable_set(df, mapping, label_col=label_col, derived=derived, to_unit=to_unit)

    # Synthesise rows for any fallback tokens absent from the loaded data.
    # A fallback entry must declare a ``value``; ``unit`` defaults to ``to_unit`` if omitted.
    fallback = spec.get("fallback", {})
    if fallback:
        present = set(result[label_col].unique()) if not result.empty else set()
        yr_reg = (
            result[["year", "region"]].drop_duplicates()
            if not result.empty
            else pd.DataFrame(columns=["year", "region"])
        )
        fallback_frames = []
        for token, fb in fallback.items():
            if token in present:
                continue
            logger.warning(
                "Token %r absent from the IAMC variable set; using declared fallback: %s",
                token,
                fb.get("reason", "(no reason given)"),
            )
            if "value" not in fb:
                continue
            rows = yr_reg.copy()
            rows[label_col] = token
            rows["value"] = fb["value"]
            rows["unit"] = fb.get("unit", to_unit or "")
            fallback_frames.append(rows)
        if fallback_frames:
            result = pd.concat([result, *fallback_frames], ignore_index=True)

    return result


#: Spec shapes this data model can serve.
SPEC_SHAPES = {"variables": load_variables}
