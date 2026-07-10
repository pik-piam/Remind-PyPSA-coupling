"""iampypsa — shared IAM↔PyPSA coupling logic and the base coupling-adapter interface.

Concrete per-model adapters live in each PyPSA model's own repository and subclass
``CouplingAdapter``.
"""

import importlib.metadata

from .adapters.base import CouplingAdapter
from .adapters.gdx import RemindGdxAdapter
from .adapters.iamc import RemindIamcAdapter
from .io import (
    RemindLoader,
    iam_name,
    build_capacity_reporting_technologies,
    default_symbol_config_path,
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
    "CouplingAdapter",
    "RemindGdxAdapter",
    "RemindIamcAdapter",
    "RemindLoader",
    "read_gdx_symbol",
    "read_ssp_data",
    "load_symbol_specs",
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
