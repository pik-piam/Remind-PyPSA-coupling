"""Model-agnostic REMIND→PyPSA data transforms.

Transforms operate on already-loaded, tidy DataFrames (canonical columns such as
``region``/``year``/``value``); they never read files or know GDX symbol names —
that wiring lives in the loader and the per-model adapter config.
"""

from __future__ import annotations

from rpycpl.transforms.co2_prices import (
    TONNE_C_TO_TONNE_CO2,
    convert_co2_prices,
    extract_co2_prices,
)
from rpycpl.transforms.costs import (
    DEFAULT_ETA_EXPONENTS,
    add_discount_rate,
    build_baseline_overrides,
    build_mapped_overrides,
    build_set_value_overrides,
    convert_investment_to_input_capacity_basis,
    apply_overrides,
)
from rpycpl.transforms.capacities import (
    adjust_link_capacities_to_input,
    aggregate_capacities_to_carriers,
    apply_consolidation,
)
from rpycpl.transforms.loads import TWA_TO_MWH, convert_loads
from rpycpl.transforms.mapping import read_region_map

__all__ = [
    "read_region_map",
    "extract_co2_prices",
    "convert_co2_prices",
    "TONNE_C_TO_TONNE_CO2",
    "convert_loads",
    "TWA_TO_MWH",
    "apply_consolidation",
    "adjust_link_capacities_to_input",
    "aggregate_capacities_to_carriers",
    "build_mapped_overrides",
    "build_baseline_overrides",
    "build_set_value_overrides",
    "convert_investment_to_input_capacity_basis",
    "add_discount_rate",
    "apply_overrides",
    "DEFAULT_ETA_EXPONENTS",
]
