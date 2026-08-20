"""Registry of the IAMs this package can couple.

One entry per IAM, holding where its packaged data lives and which code reads it. Entries are
**pure data**: coupler classes and region-map readers are named as ``"module:attribute"``
strings and imported on demand, so the registry never imports back into the package at module
level. Adding an IAM is one ``models/<iam>/`` directory plus one entry here.
"""

import importlib
import importlib.resources
from dataclasses import dataclass
from importlib.resources.abc import Traversable
from typing import Any


@dataclass(frozen=True)
class ModelSpec:
    """What the package knows about one IAM."""

    #: Import path of the subpackage holding this IAM's packaged YAML/CSV data.
    package: str
    #: Backend → packaged quantity-spec YAML filename.
    quantity_configs: dict[str, str]
    #: Backend → ``"module:ClassName"`` of the coupler serving it.
    couplers: dict[str, str]
    #: ``"module:function"`` returning the packaged region → countries map, if any.
    region_map_reader: str | None = None


#: Model assumed when a caller names none — the only IAM currently coupled end to end.
DEFAULT_MODEL = "remind"

MODELS: dict[str, ModelSpec] = {
    "remind": ModelSpec(
        package="iampypsa.models.remind",
        quantity_configs={"gdx": "quantities_gdx.yaml", "iamc": "quantities_mif.yaml"},
        couplers={
            "gdx": "iampypsa.models.remind.coupler:RemindGdxCoupler",
            "iamc": "iampypsa.models.remind.coupler:RemindIamcCoupler",
        },
        region_map_reader="iampypsa.models.remind.coupler:read_region_map",
    ),
    "iamc": ModelSpec(
        package="iampypsa.models.iamc",
        quantity_configs={"iamc": "quantities.yaml"},
        couplers={"iamc": "iampypsa.models.iamc.coupler:IamcCoupler"},
    ),
}


def get_model_spec(model: str) -> ModelSpec:
    """Look up a registered IAM; raise with the known names if absent."""
    if model not in MODELS:
        raise ValueError(f"Unknown model {model!r}; registered models: {sorted(MODELS)}.")
    return MODELS[model]


def import_target(target: str) -> Any:
    """Import a ``"module:attribute"`` reference from the registry."""
    module, _, attribute = target.partition(":")
    return getattr(importlib.import_module(module), attribute)


def get_default_config_path(model: str, backend: str) -> Traversable:
    """Return the packaged default quantity-spec YAML for ``model`` and ``backend``.

    ``backend`` is required and must be known. There is deliberately no fallback: letting a
    typo, or a forgotten argument on an IAMC run, resolve GDX names against a mif surfaces far
    from the cause, or not at all. Pass ``backend=loader.backend``.
    """
    spec = get_model_spec(model)
    if backend not in spec.quantity_configs:
        raise ValueError(
            f"Model {model!r} has no packaged config for backend {backend!r}; "
            f"it serves {sorted(spec.quantity_configs)}."
        )
    return importlib.resources.files(spec.package).joinpath(spec.quantity_configs[backend])


def get_coupler_class(model: str, backend: str) -> type:
    """Return the coupler class serving ``model`` on ``backend``."""
    spec = get_model_spec(model)
    if backend not in spec.couplers:
        raise ValueError(
            f"Model {model!r} has no coupler for backend {backend!r}; "
            f"it serves {sorted(spec.couplers)}."
        )
    return import_target(spec.couplers[backend])


def read_default_region_map(model: str) -> dict[str, list[str]]:
    """Read the model's packaged region → countries map; empty if it ships none."""
    spec = get_model_spec(model)
    if spec.region_map_reader is None:
        return {}
    return import_target(spec.region_map_reader)(source="model_region", target="country")
