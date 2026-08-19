"""Source formats: one module per data model or container.

- ``gdx``  — the GDX *container* (byte layout), read via ``gamspy``.
- ``gams`` — the GAMS *data model* (sets, parameters, domain columns), container-agnostic.
- ``iamc`` — the IAMC exchange model and its ``;``-separated file.

Adding a format is one module plus one ``SUFFIX_BACKENDS`` entry — no loader edit.
"""

from os import PathLike
from pathlib import Path
from typing import Literal

Backend = Literal["gdx", "iamc"]

#: Source-file suffix → the backend that reads it.
SUFFIX_BACKENDS: dict[str, Backend] = {".gdx": "gdx", ".mif": "iamc", ".csv": "iamc"}


def detect_backend(source: str | PathLike) -> Backend:
    """Infer the backend from a source path's suffix."""
    suffix = Path(source).suffix.lower()
    if suffix not in SUFFIX_BACKENDS:
        raise ValueError(
            f"Cannot infer backend from suffix {suffix!r} ({source}); "
            f"known suffixes: {sorted(SUFFIX_BACKENDS)}."
        )
    return SUFFIX_BACKENDS[suffix]
