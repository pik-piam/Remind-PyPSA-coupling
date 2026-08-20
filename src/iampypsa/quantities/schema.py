"""Pure schema logic over config dicts: quantity-spec shapes and technology-parameter maps.

No I/O and no imports from the rest of the package, so ``transforms`` and ``formats`` may both
depend on it without inverting the layering. Technology-mapping schema: see
``examples/technology-mapping.example.yaml``.
"""

from collections.abc import Sequence
from typing import Any

#: The 7 standard techno-economic parameters every technology entry resolves to.
STANDARD_PARAMETERS = (
    "investment", "FOM", "VOM", "efficiency", "lifetime", "fuel", "CO2 intensity",
)


# -- quantity specs ---------------------------------------------------------


def find_spec_shape(spec: dict[str, Any]) -> str:
    """Classify a quantity spec as ``"variables"``, ``"indexed"`` or ``"simple"``."""
    if "variables" in spec:
        return "variables"
    if "index" in spec and "schema" in spec:
        return "indexed"
    return "simple"


def get_quantity_ref(spec: dict[str, Any]) -> str | Sequence[str]:
    """Return the spec's source-name reference (a name or a candidate list).

    ``name:`` is the backend-neutral key; ``symbol:`` is the GDX-flavoured alias the shipped
    YAMLs still use, accepted until they are migrated.
    """
    ref = spec.get("name", spec.get("symbol"))
    if ref is None:
        raise KeyError(f"Quantity spec has no 'name'/'symbol': {spec}")
    return ref


# -- technology mapping -----------------------------------------------------


def get_iam_name(tech: str, spec: Any) -> str:
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
