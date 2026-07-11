"""Load the technology-parameter map for cost sourcing.

Schema is documented in the model's technology-mapping YAML header. An ``IAM``-declared
(technology, parameter) pair must have matching adapter data — a gap raises, not a silent
fallback; ``PyPSA`` pulls from the model's own baseline cost table.
"""

from __future__ import annotations

from os import PathLike
from typing import Any

import yaml

#: The 7 standard techno-economic parameters every technology entry resolves to.
STANDARD_PARAMETERS = (
    "investment", "FOM", "VOM", "efficiency", "lifetime", "fuel", "CO2 intensity",
)


def load_technology_parameters(path: str | PathLike) -> dict[str, Any]:
    """Return ``{"technologies": {name: spec}}`` from the model's technology-mapping YAML."""
    with open(path) as f:
        return {"technologies": yaml.safe_load(f)}


def iam_name(tech: str, spec: Any) -> str:
    """Return the IAM technology name an entry pulls IAM values from."""
    if isinstance(spec, str):
        return tech
    return spec.get("iam_name", tech)


def build_technology_sources(spec: Any) -> dict[str, Any]:
    """Expand a raw technology entry to its ``{parameter: source}`` map.

    A bare string applies to every standard parameter; a dict's ``overrides:`` win over its
    ``source:`` (or, absent ``source:``, only ``overrides:`` parameters are sourced at all).
    """
    if isinstance(spec, str):
        return {param: spec for param in STANDARD_PARAMETERS}
    overrides = spec.get("overrides", {})
    if "source" not in spec:
        return dict(overrides)
    source = spec["source"]
    return {param: overrides.get(param, source) for param in STANDARD_PARAMETERS}
