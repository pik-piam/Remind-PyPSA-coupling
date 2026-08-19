"""The IAM→PyPSA coupling interface. This is exposed to pypsa models
and is the entry point for the coupling workflow.

- ``Coupler`` is the backend-neutral base: it holds the shared, concrete builders
(``build_co2_prices``, ``build_discount_rates``, ``downscale_country_demand``)
- it consumes the resolved quantity specs and the region map, and it contains the reference
 data (population, GDP, etc.) for downscaling.

Concrete subclasses live in ``iampypsa.models.<iam>``; ``iampypsa.build_coupler`` picks the one
matching the source's model and format. A new IAM or output format is added as a further
``Coupler`` subclass there, not as a branch here.

config keys used: ``currency_factor``, ``sector_weights``, ``countries``, ``planning_horizons``.

``currency_factor`` (default ``1.0``, a no-op) is a flat multiplier the caller supplies to
convert IAM-sourced monetary values into the target PyPSA baseline's currency — it is not
looked up or computed here. It converts between currencies only, not between currency *years*
(e.g. an IAM reporting US$2017 against a baseline with its own reporting year).

TODO: once other IAMs are coupled, add a general pre-run validator confirming all data PyPSA
needs is actually present in the source (quantities, declared regions/years).
"""

import logging
from collections.abc import Sequence
from typing import Any

import pandas as pd

from iampypsa.downscale.demand import disaggregate_demand_to_country
from iampypsa.quantities.load import load_quantity, rename_technologies
from iampypsa.transforms.capacities import (
    adjust_link_capacities_to_input,
    aggregate_capacities_to_carriers,
    apply_postprocessing,
)
from iampypsa.transforms.co2_prices import extract_co2_prices
from iampypsa.transforms.costs import (
    apply_currency_factor,
    broadcast_fuel_prices,
    select_discount_rate,
)

logger = logging.getLogger(__name__)


class Coupler:
    """IAM Backend-neutral base: shared builders + source-specific hook declarations."""

    #: Drop rows whose technology has no ``technology_names`` entry — raw source tokens that
    #: ``rename_technologies`` deliberately kept as-is. Only backends with a token vocabulary
    #: to map from need this.
    drop_unmapped_technologies: bool = False

    def __init_subclass__(cls, **kwargs) -> None:
        """Warn when a subclass overrides the cost template instead of implementing its hook."""
        super().__init_subclass__(**kwargs)
        if "extract_cost_parameters" in cls.__dict__:
            logger.warning(
                "%s overrides extract_cost_parameters(); the currency factor, technology "
                "renaming and fuel-price broadcast will NOT be applied. Implement "
                "build_cost_parameters() instead and let Coupler finalise the frame.",
                cls.__name__,
            )

    # TODO type loader
    def __init__(
        self,
        loader,
        quantities: dict[str, Any],
        region_map: dict[str, list[str]],
        config: dict[str, Any],
        *,
        model_regions: list[str] | None = None,
        reference_data: dict[str, pd.DataFrame] | None = None,
    ) -> None:
        """Bind the loader, resolved quantity specs, region map, config, and reference data."""
        self.loader = loader
        self.quantities = quantities
        self.region_map = region_map
        self.config = config
        self.model_regions = model_regions or list(region_map)
        self.reference_data: dict[str, pd.DataFrame] = dict(reference_data or {})

    # -- Source-specific hooks (must be overridden by subclasses) -----------

    def build_regional_demand(self) -> pd.DataFrame:
        """Read IAM regional sectoral demand as ``[year, region, sector, value, unit]`` (MWh).

        Implemented per IAM in ``iampypsa.models``.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must implement build_regional_demand(). "
            "Use build_coupler() to get the subclass matching your source, or write one "
            "under iampypsa.models for another IAM."
        )

    def build_cost_parameters(self, year: int) -> pd.DataFrame:
        """Assemble the raw cost rows for ``year``, one group per techno-economic parameter.

        Source-specific: units and per-technology facts are resolved here, but the currency
        factor and the canonical vocabulary are not — ``extract_cost_parameters`` applies those
        to whatever this returns. Implemented per IAM in ``iampypsa.models``.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must implement build_cost_parameters(). "
            "Use build_coupler() to get the subclass matching your source, or write one "
            "under iampypsa.models for another IAM."
        )

    # -- Shared concrete builders -------------------------------------------

    def extract_cost_parameters(self, year: int) -> pd.DataFrame:
        """Extract cost parameters as long ``[region, technology, parameter, value, unit]``,
        currency-converted and on the canonical technology vocabulary.
        """
        return self.finalise_cost_parameters(self.build_cost_parameters(year))

    def get_tech_fuel_map(self) -> dict[str, str] | None:
        """Return the canonical ``technology -> priced fuel`` map, or None if the IAM has none.

        Config-driven by default; override it where the map has to be derived from the source
        rather than declared in the YAML.
        """
        return self.quantities.get("tech_fuel_map")

    def finalise_cost_parameters(self, costs: pd.DataFrame) -> pd.DataFrame:
        """Apply the currency factor, canonicalise technologies, broadcast fuel prices and
        restrict to the coupled regions.

        The single output boundary for every cost table, so no coupler can emit one that skipped
        the currency conversion or the vocabulary rename.
        """
        costs = apply_currency_factor(costs, self.config.get("currency_factor", 1.0))
        names = self.quantities.get("technology_names")
        costs = rename_technologies(costs, names)
        costs = broadcast_fuel_prices(costs, self.get_tech_fuel_map())
        if self.drop_unmapped_technologies and names:
            costs = costs[costs["technology"].isin(set(names.values()))]
        return costs[costs["region"].isin(set(self.model_regions))].reset_index(drop=True)

    def build_co2_prices(self, years: Sequence[int] | None = None) -> pd.DataFrame:
        """Build the per-(region, year) CO2 price pathway, converted to the runtime currency.

        ``years`` reindexes the result (missing filled with 0); defaults to ``config["planning_horizons"]``.
        """
        raw = load_quantity(self.loader, self.quantities["co2_price"])
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
        rates = load_quantity(self.loader, self.quantities["discount_rate"])
        rates = rates[rates["region"].isin(self.model_regions)]
        return select_discount_rate(rates, year, self.model_regions)

    # TODO model-tech resolution? is unclear -> which model, IAM?
    # TODO Resolution is reserved for space and time, why does it come in here?
    def prepare_capacities(self) -> pd.DataFrame:
        """Read installed capacities at model-tech resolution, before carrier aggregation.

        Returns ``[year, region, technology, value, unit]``. Applies the capacity spec's
        optional ``postprocessing`` block (technology-variant merging, scaling) and puts
        link-like technologies on an input-capacity basis. Callers wanting PyPSA carriers use
        :meth:`get_capacities`; callers needing model-tech resolution (e.g. group-wise
        brownfield harmonisation) use this directly.
        """
        cap_spec = self.quantities["capacity"]
        postprocessing = dict(cap_spec.get("postprocessing", {}))
        link_techs = set(postprocessing.pop("link_techs", []))

        caps = apply_postprocessing(load_quantity(self.loader, cap_spec), **postprocessing)
        if link_techs and "efficiency_conv" in self.quantities:
            eff = load_quantity(self.loader, self.quantities["efficiency_conv"]).rename(
                columns={"value": "efficiency"}
            )
            caps = adjust_link_capacities_to_input(caps, eff, link_techs)
        return caps

    def get_capacities(
        self,
        tech_map: pd.DataFrame,
        *,
        map_tech_col: str,
        map_carrier_col: str,
        regions: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """Get IAM capacities in PyPSA-ready format (where they will become must-build
        constraints), as ``[year, region, carrier, value, unit]``.

        ``tech_map`` stays an argument because the carrier vocabulary is PyPSA-side and never
        lives in the package. The ``unit`` column reflects the capacity spec's ``to_unit``.

        Args:
            tech_map: Model technology→carrier mapping table.
            map_tech_col: Column in ``tech_map`` holding the IAM technology token.
            map_carrier_col: Column in ``tech_map`` holding the target PyPSA carrier.
            regions: IAM regions to keep; defaults to ``self.model_regions``.
        """
        regions = self.model_regions if regions is None else regions
        caps = rename_technologies(self.prepare_capacities(), self.quantities.get("technology_names"))
        caps = aggregate_capacities_to_carriers(
            caps,
            tech_map,
            map_tech_col=map_tech_col,
            map_carrier_col=map_carrier_col,
            unit=self.quantities["capacity"].get("to_unit", "MW"),
        )
        caps["year"] = caps["year"].astype(int)
        return caps[caps["region"].isin(set(regions))].reset_index(drop=True)
