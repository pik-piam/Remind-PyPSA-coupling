"""Unify the GDX and ``.mif`` backends behind one loader with candidate resolution."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from os import PathLike
from pathlib import Path
from typing import Literal

import pandas as pd

from rpycpl.io.gdx import list_gdx_symbols, read_gdx_scalar, read_gdx_symbol
from rpycpl.io.iamc import list_iamc_variables, read_iamc

Backend = Literal["gdx", "iamc"]

# A symbol reference is an exact name or an ordered list of candidate names (first one
# present wins) — lets config absorb REMIND-run/version renames across models, e.g.
# ["v32_taxCO2eq", "pm_taxCO2eq"] or ["v32_load_sector", "p32_load_sector"].
SymbolRef = str | Sequence[str]

# One interface over backends: GDX symbols and IAMC ``.mif`` variables are both addressed by
# name via ``SymbolRef`` / ``resolve_symbol``, so callers never depend on the backend. The
# ``.mif`` path is variable-based (not GDX-symbol-based) — supported in shape; its reader is a
# shell for now. Broader IAM support = add a backend behind the same ``list_symbols`` /
# ``load_symbol`` interface (and rename off "Remind" if it outgrows REMIND).
# ``detect_backend`` IS used: every PyPSA-Eur call site constructs ``RemindLoader(path)`` without
# an explicit ``backend``, so ``__init__`` infers it from the suffix (.gdx → gdx).
class RemindLoader:
    """Load REMIND symbols from a GDX file or ``.mif``, resolving name candidates."""

    def __init__(self, source: str | PathLike, backend: Backend | None = None) -> None:
        """Bind to a REMIND source; detect the backend from the suffix if unset."""
        self.source: Path = Path(source)
        self.backend: Backend = backend or self.detect_backend(self.source)

    @staticmethod
    def detect_backend(source: Path) -> Backend:
        """Infer the backend (``gdx``/``mif``) from a source path's suffix."""
        suffix = Path(source).suffix.lower()
        if suffix == ".gdx":
            return "gdx"
        if suffix in (".mif", ".csv"):
            return "iamc"
        raise ValueError(f"Cannot infer REMIND backend from suffix {suffix!r} ({source})")

    def list_symbols(self) -> list[str]:
        """List the symbols/variables available in the bound source."""
        if self.backend == "gdx":
            return list_gdx_symbols(self.source)
        return list_iamc_variables(self.source)

    def resolve_symbol(self, symbol: SymbolRef) -> str:
        """Pick the first candidate name actually present in the source; raise if none."""
        candidates = [symbol] if isinstance(symbol, str) else list(symbol)
        available = set(self.list_symbols())
        for name in candidates:
            if name in available:
                return name
        raise KeyError(
            f"None of {candidates} found in {self.source.name}. "
            f"First available: {sorted(available)[:10]}"
        )

    def load_symbol(
        self,
        symbol: SymbolRef,
        rename_columns: Mapping[str, str] | None = None,
    ) -> pd.DataFrame:
        """Resolve a symbol reference (name or candidates), then load it."""
        name = self.resolve_symbol(symbol)
        if self.backend == "gdx":
            return read_gdx_symbol(self.source, name, rename_columns)
        df = read_iamc(self.source, [name])
        return df.rename(columns=dict(rename_columns)) if rename_columns else df

    def load_scalar(self, symbol: SymbolRef) -> float | str:
        """Load a scalar/string symbol (e.g. model version, run name)."""
        name = self.resolve_symbol(symbol)
        if self.backend == "gdx":
            return read_gdx_scalar(self.source, name)
        return read_iamc(self.source, [name])["value"].iloc[0]
