"""Centralized IAM→PyPSA unit conventions — one place for every conversion factor.

Conversion numbers live here, never as literals in the transforms or the adapter, so that
(a) a factor like ``1e6`` has a named, documented home, and (b) switching to another IAM means
supplying a different table, not hunting through the code.

The single source of truth is ``UNIT_CONVERSIONS``: a ``(from_unit, to_unit) → factor`` table.
The symbol YAML declares each quantity's source/target unit (``unit``/``units`` + ``to_unit``,
or per-row in a ``schema``) and the loader resolves the factor through ``unit_factor``. Identical
units convert with factor 1.0 and need no table entry.

Naming note: molar masses use ``MOLAR_MASS_*`` (g/mol) — never ``MW``, which here clashes with
megawatts.
"""

from __future__ import annotations

#: Molar masses (g/mol) used for carbon↔CO2 mass conversions.
MOLAR_MASS_C = 12.0
MOLAR_MASS_CO2 = MOLAR_MASS_C + 2 * 16.0  # 44 g/mol

#: Some sources report carbon prices/intensities per tonne of *carbon* rather than CO2; PyPSA wants per tonne CO2.
TONNE_C_TO_TONNE_CO2 = MOLAR_MASS_C / MOLAR_MASS_CO2

#: Hours per year — some sources report flows per year-average (e.g. TWa), PyPSA per MWh.
#: Convention is exactly 8760 (not 8766); pint would use 8766 and drift ~0.07%.
HOURS_PER_YEAR = 8760.0

#: The conversion table: ``(from_unit, to_unit) → multiplicative factor``. YAML unit strings
#: must match these keys. Add a row here to support a new unit pair; another IAM ships its own
#: table. Identical (from == to) pairs are handled by ``unit_factor`` and omitted here.
UNIT_CONVERSIONS: dict[tuple[str, str], float] = {
    ("$/tC", "$/tCO2"): TONNE_C_TO_TONNE_CO2,  # carbon price → CO2 price
    ("TW", "MW"): 1e6,  # capacity
    ("GW", "MW"): 1e3,  # capacity
    ("TWh", "MWh"): 1e6,
    ("TWa", "MWh"): 1e6 * HOURS_PER_YEAR,  # year-average power → annual energy (demand)
    ("EJ/yr", "MWh"): 1e18 / 3.6e9,  # annual energy flow → MWh (demand)
    ("T$/TW", "$/MW"): 1e6,  # investment (capex)
    ("T$/TWh", "$/MWh"): 1e6,  # storage investment
    ("T$/TWa", "$/MWh"): 1e6 / HOURS_PER_YEAR,  # VOM / fuel price
    ("p.u.", "%/yr"): 100.0,  # FOM expressed as fraction of capex/yr → percent
    ("Gt_C/TWa", "t_CO2/MWh"): 1e9 * (MOLAR_MASS_CO2 / MOLAR_MASS_C) / HOURS_PER_YEAR / 1e6,  # CO2 intensity
    # Cost units expressed in a base year's currency (parity assumed: US$2017 ≈ USD at default currency_factor=1.0)
    ("US$2017/kW", "USD/MW"): 1e3,      # capex
    ("US$2017/kW/yr", "USD/MW/yr"): 1e3,  # absolute FOM
    ("US$2017/GJ", "USD/MWh"): 3.6,     # VOM / fuel price (1 MWh = 3.6 GJ)
    # lifetime unit vs canonical
    ("years", "yr"): 1.0,
    # efficiency reported as a percentage, convert to per-unit for a canonical basis
    ("%", "p.u."): 0.01,
}


def unit_factor(from_unit: str, to_unit: str) -> float:
    """Return the multiplicative factor converting ``from_unit`` → ``to_unit``.

    Identical units return 1.0. An undeclared pair raises (fail loud, not silently wrong) —
    add it to ``UNIT_CONVERSIONS``.
    """
    if from_unit == to_unit:
        return 1.0
    try:
        return UNIT_CONVERSIONS[(from_unit, to_unit)]
    except KeyError:
        raise KeyError(
            f"No unit conversion defined for {from_unit!r} -> {to_unit!r}. "
            f"Add it to iampypsa.units.UNIT_CONVERSIONS."
        ) from None


#: Default efficiency exponents for output→input capacity-basis conversion (per tech).
DEFAULT_ETA_EXPONENTS: dict[str, float] = {"electrolysis": 1.0, "battery inverter": 0.5}
