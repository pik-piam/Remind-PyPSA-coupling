"""Model-agnostic IAM→PyPSA data transforms.

Transforms operate on already-loaded DataFrames with canonical columns (such as
``region``/``year``/``value``); they never read files or know GDX symbol names —
that wiring lives in the loader and the Coupler's config.
"""

from iampypsa.transforms.co2_prices import extract_co2_prices
from iampypsa.transforms.costs import (
    add_discount_rate,
    annotate_cost_rows,
    apply_currency_factor,
    broadcast_fuel_prices,
    build_pypsa_techdata,
    build_iam_techdata,
    build_fixed_value_overrides,
    convert_investment_to_input_capacity_basis,
    apply_overrides,
    select_discount_rate,
)
from iampypsa.transforms.capacities import (
    adjust_link_capacities_to_input,
    aggregate_capacities_to_carriers,
    apply_postprocessing,
)

__all__ = [
    "extract_co2_prices",
    "apply_postprocessing",
    "adjust_link_capacities_to_input",
    "aggregate_capacities_to_carriers",
    "annotate_cost_rows",
    "apply_currency_factor",
    "broadcast_fuel_prices",
    "build_iam_techdata",
    "build_pypsa_techdata",
    "build_fixed_value_overrides",
    "convert_investment_to_input_capacity_basis",
    "add_discount_rate",
    "apply_overrides",
    "select_discount_rate",
]
