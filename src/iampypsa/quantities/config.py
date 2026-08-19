"""Read and layer the quantity-spec YAML: coupling name → source name(s), units, overrides.

1. the packaged default ships with the model (``iampypsa/models/<model>/quantities_*.yaml``),
   selected by ``backend``.
2. a model/run may overlay its own YAML — passed as ``path=`` — which is **deep-merged on top**
   of the default, so the overlay only needs to list what differs (a new quantity, a renamed
   candidate, a region override).
"""

from os import PathLike
from typing import Any

import yaml

from iampypsa.formats import Backend
from iampypsa.models import DEFAULT_MODEL, get_default_config_path


def merge_configs(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Deep-merge an overlay config onto a base at the (default / overrides[region]) level."""
    out = {
        "default": dict(base.get("default", {})),
        "overrides": {r: dict(v) for r, v in base.get("overrides", {}).items()},
    }
    out["default"].update(overlay.get("default", {}))
    for region, specs in overlay.get("overrides", {}).items():
        out["overrides"].setdefault(region, {}).update(specs)
    return out


def read_quantity_config(
    path: str | PathLike | None = None,
    *,
    backend: Backend,
    model: str = DEFAULT_MODEL,
) -> dict[str, Any]:
    """Read the raw quantity config (``{default, overrides}``), overlaying a user file if any.

    Args:
        path: Overlay YAML to deep-merge onto the packaged default.
        backend: ``"gdx"`` or ``"iamc"`` — required, see ``models.get_default_config_path``.
        model: IAM whose packaged default to start from.
    """
    base = yaml.safe_load(get_default_config_path(model, backend).read_text())
    if path:
        with open(path) as f:
            base = merge_configs(base, yaml.safe_load(f))
    return base


def merge_region_overrides(config: dict[str, Any], region: str | None) -> dict[str, Any]:
    """Merge ``default`` with ``overrides[region]`` per coupling name (region entry wins).

    Pure dict logic (no I/O): ``region=None`` returns ``default`` unchanged; an unknown region
    (absent from ``overrides``) also returns ``default``. Call directly to debug the merge.
    """
    merged = dict(config["default"])
    if region is not None:
        merged.update(config.get("overrides", {}).get(region, {}))
    return merged


def load_quantity_specs(
    region: str | None = None,
    path: str | PathLike | None = None,
    *,
    backend: Backend,
    model: str = DEFAULT_MODEL,
) -> dict[str, Any]:
    """Return the resolved quantity-spec map for ``model``, ``backend`` and ``region``.

    Args:
        region: IAM region whose ``overrides:`` block wins over ``default:``; ``None`` for
            the defaults alone. Positional, for existing callers.
        path: Overlay YAML to deep-merge onto the packaged default. Positional, as above.
        backend: ``"gdx"`` or ``"iamc"`` — required. Pass ``backend=loader.backend``.
        model: IAM whose packaged default to start from.
    """
    return merge_region_overrides(
        read_quantity_config(path, backend=backend, model=model), region
    )


def load_technology_parameters(path: str | PathLike) -> dict[str, Any]:
    """Return ``{"technologies": {name: spec}}`` from the model's technology-mapping YAML."""
    with open(path) as f:
        return {"technologies": yaml.safe_load(f)}
