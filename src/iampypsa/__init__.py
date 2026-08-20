"""iampypsa — shared IAM↔PyPSA coupling logic and the ``Coupler`` interface.

The front door is :func:`build_coupler`: give it an IAM output file and it returns the coupler
matching that file's model and format, with the model's packaged quantity specs and region map
already resolved. From there, ``build_*``/``extract_*`` produce PyPSA-ready frames.

    >>> coupler = build_coupler("iam_output.gdx", config=cfg)
    >>> coupler.build_co2_prices()

Everything below this facade is importable but is package internals: :mod:`iampypsa.formats`
(how a source is read), :mod:`iampypsa.quantities` (what a coupling name means),
:mod:`iampypsa.transforms` (pure frame→frame), :mod:`iampypsa.models` (per-IAM knowledge).
"""

import importlib.metadata
from os import PathLike
from typing import Any

import pandas as pd

from iampypsa.coupler import Coupler
from iampypsa.loader import IamLoader
from iampypsa.models import DEFAULT_MODEL, get_coupler_class, read_default_region_map
from iampypsa.quantities.config import load_quantity_specs

__all__ = ["build_coupler", "Coupler", "IamLoader", "load_quantity_specs", "__version__"]


def build_coupler(
    source: str | PathLike,
    *,
    model: str = DEFAULT_MODEL,
    region: str | None = None,
    quantities_path: str | PathLike | None = None,
    region_map: dict[str, list[str]] | None = None,
    config: dict[str, Any] | None = None,
    model_regions: list[str] | None = None,
    reference_data: dict[str, pd.DataFrame] | None = None,
) -> Coupler:
    """Open an IAM source and return the coupler matching its model and format.

    Pairs the detected backend with its coupler class and packaged quantity-spec YAML, so
    callers never select the two by hand. Direct construction stays available — this is a
    convenience, not a gate.

    Args:
        source: IAM output file; the suffix selects the backend (``.gdx`` / ``.mif`` / ``.csv``).
        model: Registered IAM, see :data:`iampypsa.models.MODELS`.
        region: IAM region whose ``overrides:`` block wins over ``default:``.
        quantities_path: Overlay YAML deep-merged onto the model's packaged specs.
        region_map: ``{region: [country, ...]}``; defaults to the model's packaged map.
        config: Coupling config (``currency_factor``, ``sector_weights``, ``countries``,
            ``planning_horizons``).
        model_regions: IAM regions to keep; defaults to every region in ``region_map``.
        reference_data: Downscaling proxies (``population``, ``gdp``, degree days, ...).
    """
    loader = IamLoader(source)
    quantities = load_quantity_specs(
        region, quantities_path, backend=loader.backend, model=model
    )
    if region_map is None:
        region_map = read_default_region_map(model)
    if not region_map and not model_regions:
        # Both empty means every builder silently returns nothing — fail at the front door.
        raise ValueError(
            f"Model {model!r} ships no region map, so pass region_map= (or at least "
            "model_regions=) to say which IAM regions to couple."
        )
    coupler_cls = get_coupler_class(model, loader.backend)
    return coupler_cls(
        loader,
        quantities,
        region_map,
        config or {},
        model_regions=model_regions,
        reference_data=reference_data,
    )


try:
    __version__ = importlib.metadata.version("iam-pypsa-coupling")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"
