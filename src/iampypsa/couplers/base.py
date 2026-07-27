"""The IAM→PyPSA coupling interface. This is exposed to pypsa models
and is the entry point for the coupling workflow.

- ``Coupler`` is the backend-neutral base: it holds the shared, concrete builders
(``build_co2_prices``, ``build_discount_rates``, ``downscale_country_demand``)
- it consumes the IAM symbols (resolved via their config) and the region map, and it contains
 the reference data (population, GDP, etc.) for downscaling.

Concrete subclasses are instantiated directly by the caller, which selects on ``loader.backend``;
a new IAM or output format is added as a further ``Coupler`` subclass, not a branch here:
- ``RemindGdxCoupler``  (``iampypsa.couplers.remind``)
- ``RemindIamcCoupler`` (``iampypsa.couplers.remind``)

config keys used: ``currency_factor``, ``sector_weights``, ``countries``, ``planning_horizons``.

``currency_factor`` (default ``1.0``, a no-op) is a flat multiplier the caller supplies to
convert IAM-sourced (REMIND: USD) monetary values into the target PyPSA baseline's currency —
it is not looked up or computed here. It converts between currencies only, not between
currency *years* (e.g. REMIND's US$2017 vs the baseline's own reporting year).
"""

import logging
from collections.abc import Sequence
from typing import Any

import pandas as pd

from iampypsa.downscale.demand import disaggregate_demand_to_country
from iampypsa.io.remind_symbols import load_frame, load_spec, rename_technologies
from iampypsa.transforms.capacities import (
    adjust_link_capacities_to_input,
    aggregate_capacities_to_carriers,
    apply_consolidation,
)
from iampypsa.transforms.co2_prices import extract_co2_prices
from iampypsa.transforms.costs import apply_currency_factor, select_discount_rate

logger = logging.getLogger(__name__)


class Coupler:
    """Backend-neutral base: shared builders + source-specific hook declarations."""

    def __init__(
        self,
        loader,
        symbols: dict[str, Any],
        region_map: dict[str, list[str]],
        config: dict[str, Any],
        *,
        model_regions: list[str] | None = None,
        reference_data: dict[str, pd.DataFrame] | None = None,
    ) -> None:
        """Bind the loader, resolved symbol map, region map, config, and reference data."""
        self.loader = loader
        self.symbols = symbols
        self.region_map = region_map
        self.config = config
        self.model_regions = model_regions or list(region_map)
        self.reference_data: dict[str, pd.DataFrame] = dict(reference_data or {})

    # -- Source-specific hooks (must be overridden by subclasses) -----------

    def build_regional_demand(self) -> pd.DataFrame:
        """Read IAM regional sectoral demand as ``[year, region, sector, value, unit]`` (MWh).

        Implemented by ``RemindGdxCoupler`` / ``RemindIamcCoupler``.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must implement build_regional_demand(). "
            "Instantiate RemindGdxCoupler or RemindIamcCoupler (per loader.backend), "
            "or a new Coupler subclass for another IAM."
        )

    def extract_cost_parameters(self, year: int) -> pd.DataFrame:
        """Extract cost parameters as long ``[region, reference, parameter, value, unit]``.

        Implemented by ``RemindGdxCoupler`` / ``RemindIamcCoupler``.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must implement extract_cost_parameters(). "
            "Instantiate RemindGdxCoupler or RemindIamcCoupler (per loader.backend), "
            "or a new Coupler subclass for another IAM."
        )

    # -- Shared concrete builders -------------------------------------------

    def build_co2_prices(self, years: Sequence[int] | None = None) -> pd.DataFrame:
        """Build the per-(region, year) CO2 price pathway, converted to the runtime currency.

        ``years`` reindexes the result (missing filled with 0); defaults to ``config["planning_horizons"]``.
        """
        raw = load_frame(self.loader, self.symbols["co2_price"])
        years = years if years is not None else self.config.get("planning_horizons")
        prices = extract_co2_prices(raw, regions=self.model_regions, years=years)
        # parameters=None, not a CURRENCY_COST_PARAMETERS entry: this frame is single-quantity
        # (no `parameter` column), so every row is the monetary one.
        return apply_currency_factor(
            prices, self.config.get("currency_factor", 1.0), parameters=None
        )

    def downscale_country_demand(self, regional: pd.DataFrame | None = None) -> pd.DataFrame:
        """Downscale IAM regional demand to per-country annual demand by sector and year.

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

    def build_discount_rates(self, year: int) -> pd.Series:
        """Return the discount rate per region for ``year``, indexed by region."""
        rates = load_frame(self.loader, self.symbols["discount_rate"])
        rates = rates[rates["region"].isin(self.model_regions)]
        return select_discount_rate(rates, year, self.model_regions)

    def prepare_capacities(self) -> pd.DataFrame:
        """Read installed capacities at model-tech resolution, before carrier aggregation.

        Returns ``[year, region, technology, value, unit]``. Applies the capacity spec's
        optional ``consolidation`` block (VRE-variant merging, battery scaling) and puts
        link-like technologies on an input-capacity basis. Callers wanting PyPSA carriers use
        :meth:`build_capacity_targets`; callers needing model-tech resolution (e.g. group-wise
        brownfield harmonisation) use this directly.
        """
        cap_spec = self.symbols["capacity"]
        consolidation = dict(cap_spec.get("consolidation", {}))
        link_techs = set(consolidation.pop("link_techs", []))

        caps = apply_consolidation(load_spec(self.loader, cap_spec), **consolidation)
        if link_techs and "efficiency_conv" in self.symbols:
            eff = load_spec(self.loader, self.symbols["efficiency_conv"]).rename(
                columns={"value": "efficiency"}
            )
            caps = adjust_link_capacities_to_input(caps, eff, link_techs)
        return caps

    def build_capacity_targets(
        self,
        tech_map: pd.DataFrame,
        *,
        map_tech_col: str,
        map_carrier_col: str,
        regions: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """Build installed-capacity targets as ``[year, region, carrier, value, unit]``.

        ``tech_map`` stays an argument because the carrier vocabulary is PyPSA-side and never
        lives in the package. The ``unit`` column reflects the capacity spec's ``to_unit``.

        Args:
            tech_map: Model technology→carrier mapping table.
            map_tech_col: Column in ``tech_map`` holding the IAM technology token.
            map_carrier_col: Column in ``tech_map`` holding the target PyPSA carrier.
            regions: IAM regions to keep; defaults to ``self.model_regions``.
        """
        regions = self.model_regions if regions is None else regions
        caps = rename_technologies(self.prepare_capacities(), self.symbols.get("technology_names"))
        caps = aggregate_capacities_to_carriers(
            caps,
            tech_map,
            map_tech_col=map_tech_col,
            map_carrier_col=map_carrier_col,
            unit=self.symbols["capacity"].get("to_unit", "MW"),
        )
        caps["year"] = caps["year"].astype(int)
        return caps[caps["region"].isin(set(regions))].reset_index(drop=True)
