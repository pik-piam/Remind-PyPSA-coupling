"""Read and layer the quantity-spec YAML: coupling name → source name(s), units, overrides.

1. the packaged default ships with the model (``iampypsa/models/<model>/quantities_*.yaml``),
   selected by ``backend``.
2. a model/run may overlay its own YAML — passed as ``path=`` — which is **deep-merged on top**
   of the default, so the overlay only needs to list what differs (a new quantity, a renamed
   candidate, a region override).
"""

import logging
from os import PathLike
from typing import Any

import yaml

from iampypsa.formats import Backend
from iampypsa.models import DEFAULT_MODEL, get_default_config_path
from iampypsa.units import parse_currency_year

logger = logging.getLogger(__name__)


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
    quantities = merge_region_overrides(
        read_quantity_config(path, backend=backend, model=model), region
    )
    check_currency_consistency(quantities)
    return quantities


def find_declared_units(spec: Any) -> list[str]:
    """Return every unit string a spec declares, across all three spec shapes."""
    if not isinstance(spec, dict):
        return []
    units = [spec.get("unit"), spec.get("to_unit"), *spec.get("units", [])]
    for entry in spec.get("schema", {}).values():
        units += [entry.get("unit"), entry.get("to_unit")]
    return [u for u in units if isinstance(u, str)]


def check_currency_consistency(quantities: dict[str, Any]) -> None:
    """Warn when a spec declares a currency year other than the one the config declares.

    Values are never deflated between currency years, so a config that mixes them is silently
    wrong rather than loud. Catches the realistic case: a run moves to a new currency year and
    only some specs get updated.
    """
    declared = quantities.get("currency", {}).get("year")
    if declared is None:
        return
    mismatched = {
        name: year
        for name, spec in quantities.items()
        for unit in find_declared_units(spec)
        if (year := parse_currency_year(unit)) is not None and year != declared
    }
    if mismatched:
        logger.warning(
            "Quantity specs declare currency years other than the config's US$%d: %s. "
            "Values are not deflated between years — the mismatched ones are wrong.",
            declared,
            mismatched,
        )


def load_technology_parameters(path: str | PathLike) -> dict[str, Any]:
    """Return ``{"technologies": {name: spec}}`` from the model's technology-mapping YAML."""
    with open(path) as f:
        return {"technologies": yaml.safe_load(f)}
