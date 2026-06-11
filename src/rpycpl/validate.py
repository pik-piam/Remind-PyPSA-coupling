"""Validate a config-declared scenario against the actual REMIND output.

Supports the config-declared invocation model: the user lists regions/years in config and
this fails loudly before any prep runs if they are not present in the REMIND source.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from rpycpl.io.loader import RemindLoader
from rpycpl.io.remind_symbols import load_frame


def validate_scenario_against_remind(
    loader: RemindLoader,
    symbols: dict[str, Any],
    declared_regions: Sequence[str],
    declared_years: Sequence[int],
) -> None:
    """Raise if any declared region/year is absent from the REMIND source.

    Years are read from the ``coupled_years`` symbol; regions from the ``co2_price`` frame
    (any region-bearing symbol would do). Raises ``ValueError`` listing what is missing.
    """
    years_frame = load_frame(loader, symbols["coupled_years"])
    available_years = {int(y) for y in years_frame["year"]}
    missing_years = {int(y) for y in declared_years} - available_years

    region_frame = load_frame(loader, symbols["co2_price"])
    available_regions = set(region_frame["region"])
    missing_regions = set(declared_regions) - available_regions

    problems = []
    if missing_regions:
        problems.append(f"regions {sorted(missing_regions)} (available: {sorted(available_regions)})")
    if missing_years:
        problems.append(f"years {sorted(missing_years)} (available: {sorted(available_years)})")
    if problems:
        raise ValueError(
            "Config-declared scenario disagrees with the REMIND source: " + "; ".join(problems)
        )
