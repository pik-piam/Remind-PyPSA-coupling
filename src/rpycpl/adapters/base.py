"""The IAM→PyPSA coupling adapter interface.

``CouplingAdapter`` is the backend-neutral base: it holds the shared, concrete builders
(``build_co2_prices``, ``discount_rates``, ``downscale_country_demand``)
driven by the resolved symbol map and config, and declares the two source-specific hooks
(``build_regional_demand``, ``extract_cost_parameters``) as abstract-style methods that
raise ``NotImplementedError``.

Concrete adapters for each REMIND output format (instantiated directly by the caller,
which selects on ``loader.backend``):
- ``RemindGdxAdapter``  (``rpycpl.adapters.gdx``)
- ``RemindIamcAdapter`` (``rpycpl.adapters.iamc``)

config keys used: ``currency_factor``, ``sector_weights``, ``countries``, ``planning_horizons``.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

from rpycpl.downscale.demand import disaggregate_demand_to_country
from rpycpl.io.remind_symbols import load_frame
from rpycpl.transforms.co2_prices import convert_co2_prices, extract_co2_prices
from rpycpl.transforms.costs import (
    convert_investment_to_input_capacity_basis,
    apply_overrides,
)


class CouplingAdapter:
    """Backend-neutral base: shared builders + source-specific hook declarations."""

    def __init__(
        self,
        loader,
        symbols: dict[str, Any],
        region_map: dict[str, list[str]],
        config: dict[str, Any],
        *,
        remind_regions: list[str] | None = None,
        reference_data: dict[str, pd.DataFrame] | None = None,
        ssp_population: pd.DataFrame | None = None,
        ssp_gdp: pd.DataFrame | None = None,
    ) -> None:
        """Bind the loader, resolved symbol map, region map, config, and reference data."""
        self.loader = loader
        self.symbols = symbols
        self.region_map = region_map
        self.config = config
        self.remind_regions = remind_regions or list(region_map)
        self.reference_data: dict[str, pd.DataFrame] = dict(reference_data or {})
        if ssp_population is not None:
            self.reference_data.setdefault("population", ssp_population)
        if ssp_gdp is not None:
            self.reference_data.setdefault("gdp", ssp_gdp)

    @property
    def ssp_population(self) -> pd.DataFrame | None:
        return self.reference_data.get("population")

    @property
    def ssp_gdp(self) -> pd.DataFrame | None:
        return self.reference_data.get("gdp")

    # -- Source-specific hooks (must be overridden by subclasses) -----------

    def build_regional_demand(self) -> pd.DataFrame:
        """Read REMIND regional sectoral demand as ``[year, region, sector, value, unit]`` (MWh).

        Implemented by ``RemindGdxAdapter`` / ``RemindIamcAdapter``.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must implement build_regional_demand(). "
            "Instantiate RemindGdxAdapter or RemindIamcAdapter (per loader.backend)."
        )

    def extract_cost_parameters(self, year: int) -> pd.DataFrame:
        """Extract cost parameters as long ``[region, reference, parameter, value, unit]``.

        Implemented by ``RemindGdxAdapter`` / ``RemindIamcAdapter``.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must implement extract_cost_parameters(). "
            "Instantiate RemindGdxAdapter or RemindIamcAdapter (per loader.backend)."
        )

    # -- Shared concrete builders -------------------------------------------

    def build_co2_prices(self, years: Sequence[int] | None = None) -> pd.DataFrame:
        """Build the per-(region, year) CO2 price pathway.

        Works for both backends: GDX spec applies tC→tCO2 conversion; IAMC spec reports
        directly in t CO2 (no conversion). The runtime ``currency_factor`` is always applied.

        ``years`` selects the year set to reindex to (missing filled with 0); when ``None``
        it falls back to ``config["planning_horizons"]``. Callers that derive the coupled-year
        set from the REMIND source (e.g. the GDX ``t`` symbol) pass it explicitly.
        """
        raw = load_frame(self.loader, self.symbols["co2_price"])
        years = years if years is not None else self.config.get("planning_horizons")
        prices = extract_co2_prices(raw, regions=self.remind_regions, years=years)
        return convert_co2_prices(
            prices, currency_factor=self.config.get("currency_factor", 1.0), carbon_to_co2=False
        )

    def downscale_country_demand(self, regional: pd.DataFrame | None = None) -> pd.DataFrame:
        """Downscale REMIND regional demand to per-country annual demand by sector and year.

        The proxy registry is ``self.reference_data`` verbatim — it already holds ``population``/
        ``gdp`` (and any ``heating_demand``/``cooling_demand`` the caller added). Each sector's
        ``sector_weights`` entry names which of those proxies to blend.
        """
        loads = self.build_regional_demand() if regional is None else regional
        if self.config.get("planning_horizons"):
            years = {int(y) for y in self.config["planning_horizons"]}
            loads = loads[loads["year"].isin(years)]
        return disaggregate_demand_to_country(
            loads,
            self.region_map,
            self.reference_data,
            self.config["sector_weights"],
            set(self.config["countries"]),
        )

    def discount_rates(self, year: int) -> pd.Series:
        """Return the REMIND discount rate per region for ``year``, indexed by region.

        Works for both backends: GDX spec reads ``p_r``; IAMC spec reads
        ``Interest Rate t/(t-1)|Real``. When a region has no value for ``year`` (e.g. NaN
        in the last mif column), the most recent earlier year's value is used with a warning.
        """
        p_r = (
            load_frame(self.loader, self.symbols["discount_rate"])
            .pipe(lambda df: df[df["region"].isin(self.remind_regions)])
        )
        # Last-value per region: covers both the requested year and any forward-fill fallback.
        last = p_r.sort_values("year").groupby("region")["value"].last()
        missing = set(self.remind_regions) - set(last.index)
        if missing:
            raise ValueError(
                f"No REMIND discount rate for year {year} or any earlier year, "
                f"regions: {sorted(missing)}"
            )
        exact = p_r[p_r["year"].astype(str) == str(year)].set_index("region")["value"]
        filled = last.index.difference(exact.index)
        if len(filled):
            logger.warning(
                "Discount rate absent for year %d in regions %s; using most-recent value.",
                year, sorted(filled),
            )
        return exact.combine_first(last)
