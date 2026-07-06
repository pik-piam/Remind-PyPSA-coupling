"""REMIND input readers (GDX + IAMC ``.mif``) behind one loader, plus the config-aware
symbol layer (``remind_symbols``) that resolves logical names + units on top of the loader."""

from __future__ import annotations

from rpycpl.io.gdx import list_gdx_symbols, read_gdx_scalar, read_gdx_symbol
from rpycpl.io.iamc import list_iamc_variables, read_iamc
from rpycpl.io.loader import Backend, RemindLoader, SymbolRef
from rpycpl.io.remind_symbols import (
    SYMBOL_CONFIG_ENV,
    default_symbol_config_path,
    load_frame,
    load_set,
    load_spec,
    load_symbol_specs,
    load_variable_set,
    merge_region_overrides,
    read_symbol_config,
    report_fallbacks,
)
from rpycpl.io.degree_days import read_degree_days
from rpycpl.io.ssp import fetch_ssp_data, fetch_ssp_variable, read_ssp_data

__all__ = [
    "RemindLoader",
    "Backend",
    "SymbolRef",
    "read_gdx_symbol",
    "read_gdx_scalar",
    "list_gdx_symbols",
    "read_iamc",
    "list_iamc_variables",
    "fetch_ssp_data",
    "fetch_ssp_variable",
    "read_ssp_data",
    "read_degree_days",
    "load_symbol_specs",
    "load_spec",
    "load_variable_set",
    "read_symbol_config",
    "merge_region_overrides",
    "default_symbol_config_path",
    "load_frame",
    "load_set",
    "report_fallbacks",
    "SYMBOL_CONFIG_ENV",
]
