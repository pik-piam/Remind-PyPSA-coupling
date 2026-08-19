"""Bind one IAM source file and resolve quantity names against it.

``IamLoader`` answers three questions and nothing else: what kind of source is this
(``backend``), what names does it contain (``list_names``), and which of a spec's candidate
names is actually present (``resolve``). Units, coupling names, spec shapes, renames and
fallbacks all belong to :mod:`iampypsa.quantities`.

Vocabulary: ``read_*`` is raw access by the source's own native name; ``load_*`` is
spec-driven, canonical and unit-converted.
"""

from collections.abc import Mapping, Sequence
from os import PathLike
from pathlib import Path

import pandas as pd

from iampypsa.formats import Backend, detect_backend
from iampypsa.formats.gdx import list_gdx_symbols, read_gdx_scalar, read_gdx_symbol
from iampypsa.formats.iamc import list_iamc_variables, read_iamc

# A quantity reference is an exact name or an ordered list of candidate names (first one
# present wins) — lets config absorb model-specific variable renames across versions, e.g.
# ["v32_taxCO2eq", "pm_taxCO2eq"] or ["v32_load_sector", "p32_load_sector"].
QuantityRef = str | Sequence[str]


class IamLoader:
    """Bind an IAM source file and resolve names in it, whatever its format."""

    def __init__(self, source: str | PathLike, backend: Backend | None = None) -> None:
        """Bind to a source file; detect the backend from the suffix if unset."""
        self.source: Path = Path(source)
        self.backend: Backend = backend or detect_backend(self.source)

    def list_names(self) -> list[str]:
        """List the names available in the bound source (GDX symbols or IAMC variables)."""
        if self.backend == "gdx":
            return list_gdx_symbols(self.source)
        return list_iamc_variables(self.source)

    def resolve(self, ref: QuantityRef) -> str:
        """Pick the first candidate name actually present in the source; raise if none."""
        candidates = [ref] if isinstance(ref, str) else list(ref)
        available = set(self.list_names())
        for name in candidates:
            if name in available:
                return name
        raise KeyError(
            f"None of {candidates} found in {self.source.name}. "
            f"First available: {sorted(available)[:10]}"
        )

    def read(
        self,
        ref: QuantityRef,
        rename_columns: Mapping[str, str] | None = None,
    ) -> pd.DataFrame:
        """Resolve a name reference, then read it raw."""
        name = self.resolve(ref)
        if self.backend == "gdx":
            return read_gdx_symbol(self.source, name, rename_columns)
        df = read_iamc(self.source, [name])
        return df.rename(columns=dict(rename_columns)) if rename_columns else df

    def read_scalar(self, ref: QuantityRef) -> float | str:
        """Read a scalar/string quantity (e.g. model version, run name)."""
        name = self.resolve(ref)
        if self.backend == "gdx":
            return read_gdx_scalar(self.source, name)
        return read_iamc(self.source, [name])["value"].iloc[0]
