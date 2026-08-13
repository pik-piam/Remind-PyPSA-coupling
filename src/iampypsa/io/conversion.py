"""Apply a spec's declared unit conversion to a value column.

Shared by every io-layer loader that reads a symbol/variable against a spec declaring
``unit:``/``units:``/``to_unit:`` — keeps the "which unit, then multiply by the central
iampypsa.units factor" logic in one place instead of duplicated per loader.
"""

from typing import Any

import pandas as pd

from iampypsa.units import unit_factor


def resolve_source_unit(spec: dict[str, Any], ref, resolved_name: str, df: pd.DataFrame) -> str | None:
    """Resolve the source unit for a single-quantity spec.

    Prefers a live ``unit`` column on ``df`` (mif); raises on heterogeneous live values or a
    stale declared ``unit:``. Falls back to a declared ``units:``/``unit:`` when no live column
    exists (GDX). Returns None if neither is available.
    """
    if "unit" in df.columns:
        live = df["unit"].dropna().unique()
        if len(live) > 1:
            raise ValueError(f"Heterogeneous units for {resolved_name!r}: {sorted(live)}.")
        if len(live) == 1:
            declared = spec.get("unit")
            if declared is not None and declared != live[0]:
                raise ValueError(
                    f"Declared unit {declared!r} for {resolved_name!r} does not match "
                    f"the live mif unit {live[0]!r} — update the yaml's ``unit:``."
                )
            return live[0]
    if "units" in spec:
        candidates = [ref] if isinstance(ref, str) else list(ref)
        return spec["units"][candidates.index(resolved_name)]
    return spec.get("unit")


def convert_column(df: pd.DataFrame, value_col: str, src_unit: str | None, to_unit: str | None) -> pd.DataFrame:
    """Scale ``df[value_col]`` by ``unit_factor(src_unit, to_unit)``.

    No-op if either unit is ``None`` or ``value_col`` isn't present.
    """
    if to_unit is None or src_unit is None or value_col not in df.columns:
        return df
    df = df.copy()
    df[value_col] = df[value_col] * unit_factor(src_unit, to_unit)
    return df
