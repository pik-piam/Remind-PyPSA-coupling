"""Centralized REMIND→PyPSA unit conventions — one place for every magic factor.

Unit handling is intentionally collected here rather than scattered as literals across the
transforms, so that (a) a magic number like ``1e6`` always has a named, documented home, and
(b) switching to another IAM means supplying a different conversion table, not hunting through
the code. The factors below encode *REMIND's* reporting conventions; another IAM would provide
its own ``<IAM>_UNIT_CONVERSIONS`` mapping with the same keys.

Naming note: molar masses use ``MOLAR_MASS_*`` (g/mol) — never ``MW``, which here would clash
with megawatts.
"""

from __future__ import annotations

#: Molar masses (g/mol) used for carbon↔CO2 mass conversions.
MOLAR_MASS_C = 12.0
MOLAR_MASS_CO2 = MOLAR_MASS_C + 2 * 16.0  # 44 g/mol

#: REMIND reports carbon prices/intensities per tonne of *carbon*; PyPSA wants per tonne CO2.
TONNE_C_TO_TONNE_CO2 = MOLAR_MASS_C / MOLAR_MASS_CO2

#: Hours per year — REMIND reports flows per year-average (e.g. TWa), PyPSA per MWh.
HOURS_PER_YEAR = 8760.0

#: REMIND→PyPSA per-parameter conversion factors (the curated coupling-GDX defaults).
#: Keyed by cost parameter; another IAM would ship its own table with these same keys.
#: A model whose interface diverges can pass an alternative mapping into the transforms.
REMIND_UNIT_CONVERSIONS: dict[str, float] = {
    "capex": 1e6,  # T$/TW(h) -> $/MW(h)
    "VOM": 1e6 / HOURS_PER_YEAR,  # T$/TWa -> $/MWh
    "FOM": 100.0,  # p.u. -> %/year
    "co2_intensity": 1e9 * (MOLAR_MASS_CO2 / MOLAR_MASS_C) / HOURS_PER_YEAR / 1e6,  # Gt_C/TWa -> t_CO2/MWh
}

#: Default efficiency exponents for output→input capacity-basis conversion (per tech).
DEFAULT_ETA_EXPONENTS: dict[str, float] = {"electrolysis": 1.0, "battery inverter": 0.5}
