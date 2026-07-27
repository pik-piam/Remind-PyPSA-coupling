"""iampypsa — shared IAM↔PyPSA coupling logic and the ``Coupler`` interface.

Concrete ``Coupler`` subclasses are per-IAM-backend (``RemindGdxCoupler``, ``RemindIamcCoupler``
for REMIND today); most PyPSA models construct one directly. A model may further subclass
``Coupler`` in its own repository for tweaks that genuinely differ.
"""

import importlib.metadata

from .couplers.base import Coupler
from .couplers.remind import RemindGdxCoupler, RemindIamcCoupler
from .io import (
    RemindLoader,
    iam_name,
    build_capacity_reporting_technologies,
    default_symbol_config_path,
    load_frame,
    load_set,
    load_symbol_specs,
    load_spec,
    load_technology_parameters,
    load_variable_set,
    merge_region_overrides,
    read_gdx_symbol,
    read_symbol_config,
    read_ssp_data,
    rename_technologies,
    report_fallbacks,
    build_technology_sources,
)
from .validate import validate_scenario_against_remind

__all__ = [
    "Coupler",
    "RemindGdxCoupler",
    "RemindIamcCoupler",
    "RemindLoader",
    "read_gdx_symbol",
    "read_ssp_data",
    "load_symbol_specs",
    "load_frame",
    "load_set",
    "load_spec",
    "load_variable_set",
    "read_symbol_config",
    "merge_region_overrides",
    "default_symbol_config_path",
    "rename_technologies",
    "report_fallbacks",
    "load_technology_parameters",
    "iam_name",
    "build_technology_sources",
    "build_capacity_reporting_technologies",
    "validate_scenario_against_remind",
]

try:
    __version__ = importlib.metadata.version("iam-pypsa-coupling")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"
