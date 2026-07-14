"""Model-agnostic IAM→PyPSA data transforms.

Transforms operate on already-loaded, tidy DataFrames (canonical columns such as
``region``/``year``/``value``); they never read files or know GDX symbol names —
that wiring lives in the loader and the Coupler's config.
"""

from iampypsa.transforms.co2_prices import (
    TONNE_C_TO_TONNE_CO2,
    convert_co2_prices,
    extract_co2_prices,
)
from iampypsa.transforms.costs import (
    DEFAULT_ETA_EXPONENTS,
    add_discount_rate,
    build_pypsa_techdata,
    build_iam_techdata,
    build_fixed_value_overrides,
    convert_investment_to_input_capacity_basis,
    apply_overrides,
)
from iampypsa.transforms.capacities import (
    adjust_link_capacities_to_input,
    aggregate_capacities_to_carriers,
    apply_consolidation,
    build_capacity_targets,
    prepare_capacities,
)
from iampypsa.transforms.loads import TWA_TO_MWH, convert_loads

__all__ = [
    "extract_co2_prices",
    "convert_co2_prices",
    "TONNE_C_TO_TONNE_CO2",
    "convert_loads",
    "TWA_TO_MWH",
    "apply_consolidation",
    "adjust_link_capacities_to_input",
    "aggregate_capacities_to_carriers",
    "build_capacity_targets",
    "prepare_capacities",
    "build_iam_techdata",
    "build_pypsa_techdata",
    "build_fixed_value_overrides",
    "convert_investment_to_input_capacity_basis",
    "add_discount_rate",
    "apply_overrides",
    "DEFAULT_ETA_EXPONENTS",
]
