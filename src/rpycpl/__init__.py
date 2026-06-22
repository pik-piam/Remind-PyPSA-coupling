"""rpycpl — shared REMIND↔PyPSA coupling logic and the base coupling-adapter interface.

Concrete per-model adapters live in each PyPSA model's own repository and subclass
``CouplingAdapter``.
"""

import importlib.metadata

from .adapters.base import CouplingAdapter
from .adapters.gdx import RemindGdxAdapter
from .adapters.iamc import RemindIamcAdapter
from .io import (
    RemindLoader,
    default_symbol_config_path,
    load_symbol_specs,
    load_spec,
    load_variable_set,
    merge_region_overrides,
    read_gdx_symbol,
    read_symbol_config,
    read_ssp_data,
    report_fallbacks,
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
    "report_fallbacks",
    "validate_scenario_against_remind",
]

try:
    __version__ = importlib.metadata.version("REMIND-PyPSA-coupling")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"
