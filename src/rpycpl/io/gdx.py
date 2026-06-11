"""Read REMIND symbols from a GDX container (via ``gamspy``).

Ported from ``rpycpl.utils.read_gdx`` / PyPSA-Eur ``_helpers.read_remind_data``.
"""

from __future__ import annotations

import functools
from collections.abc import Mapping
from os import PathLike


@functools.lru_cache
def _open_container(path: str):
    """Open (and cache) a GDX container."""
    from gamspy import Container

    return Container(load_from=path)


def read_gdx_symbol(
    path: str | PathLike,
    symbol: str,
    rename_columns: Mapping[str, str] | None = None,
    error_on_empty: bool = True,
):
    """Read one GDX symbol into a tidy long DataFrame."""
    data = _open_container(str(path))[symbol]
    df = data.records
    if error_on_empty and (df is None or df.empty):
        raise ValueError(f"{symbol} is empty. In: {path}")
    if df is None:
        return df
    if rename_columns:
        # errors="ignore": candidate fallbacks may have different columns
        # (e.g. the v32 variable has 'level', the p32 parameter has 'value').
        df = df.rename(columns=dict(rename_columns))
    return df


def read_gdx_scalar(path: str | PathLike, symbol: str) -> float | str:
    """Read a scalar/string GDX symbol (e.g. model version, run name)."""
    df = read_gdx_symbol(path, symbol, error_on_empty=False)
    if df is None or df.empty:
        raise ValueError(f"{symbol} has no records in {path}")
    # scalar parameter -> single 'value'; string set -> first element label
    if "value" in df.columns and len(df) == 1:
        return df["value"].iloc[0]
    return df.iloc[0, 0]


def list_gdx_symbols(path: str | PathLike) -> list[str]:
    """List the symbol names available in a GDX container."""
    return sorted(_open_container(str(path)).data.keys())
