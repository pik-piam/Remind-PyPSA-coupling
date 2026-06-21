"""The IAM→PyPSA coupling adapter interface.

The builders are concrete here — they compose the shared loader + transforms + downscaler,
driven by a resolved ``symbols`` map (from ``rpycpl.io.remind_symbols.load_symbol_specs``) and a
``config`` dict. The class is **directly instantiable**: it is used as-is wherever a coupling
needs several REMIND reads through one loader (e.g. cost extraction). Model-specific tweaks can
still be added by subclassing and overriding a builder, but no override is required.

REMIND-GDX-interface specifics (capacity consolidation: VRE merge, battery scaling, link techs)
live in the symbol config (``capacity.consolidation``), not here, so they are strictly scoped to
the REMIND input and an IAMC/.mif config can omit them.

config keys used: ``currency_factor``, ``sector_weights``, ``countries``, ``planning_horizons``.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from rpycpl.downscale.demand import disaggregate_demand_to_country
from rpycpl.io.remind_symbols import load_frame, load_set
from rpycpl.transforms.co2_prices import convert_co2_prices, extract_co2_prices
from rpycpl.transforms.costs import (
    build_cost_overrides,
    convert_investment_to_input_capacity_basis,
    merge_cost_overrides_into_baseline,
)
from rpycpl.transforms.loads import convert_loads
from rpycpl.units import HOURS_PER_YEAR, unit_factor


class CouplingAdapter:
    """Standardize how a model exposes REMIND-derived inputs to PyPSA (directly instantiable)."""

    def __init__(
        self,
        loader,
        symbols: dict[str, Any],
        region_map: dict[str, list[str]],
        config: dict[str, Any],
        *,
        remind_regions: list[str] | None = None,
        # Named reference/proxy distributions, e.g. {"population": df, "gdp": df}. Open-ended on
        # purpose: any builder can look up what it needs by key, so new reference data (a new
        # downscaling proxy, a calibration series) is added without changing this signature.
        # Optional — only needed when a model has multi-country REMIND regions to split; a
        # single-country coupling (e.g. CHA → CN) may pass nothing.
        reference_data: dict[str, pd.DataFrame] | None = None,
        # Convenience aliases for the two SSP proxies; folded into reference_data below.
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
        """The SSP population proxy from ``reference_data`` (``None`` if unset)."""
        return self.reference_data.get("population")

    @property
    def ssp_gdp(self) -> pd.DataFrame | None:
        """The SSP GDP proxy from ``reference_data`` (``None`` if unset)."""
        return self.reference_data.get("gdp")

    # -- generic Stage-1 builders (all overridable) ---------------------------------

    def build_co2_prices(self) -> pd.DataFrame:
        """Build the per-(region, year) CO2 price pathway.

        The tC→tCO2 conversion is declared in the symbol config and applied by ``load_frame``,
        so here only the (runtime, config-driven) currency factor is applied.
        """
        raw = load_frame(self.loader, self.symbols["co2_price"])
        prices = extract_co2_prices(
            raw, regions=self.remind_regions, years=self.config.get("planning_horizons")
        )
        return convert_co2_prices(
            prices, currency_factor=self.config.get("currency_factor", 1.0), carbon_to_co2=False
        )

    def build_regional_demand(self) -> pd.DataFrame:
        """Read REMIND regional sectoral demand as tidy ``[year, region, sector, value]`` (MWh/yr).

        Stage 1 of the demand pipeline: read the load-sector symbol (TWa→MWh applied by
        ``load_frame``) and restrict to the configured REMIND regions. All available years are
        returned; the year restriction to the planning horizons happens in
        ``downscale_country_demand`` (Stage 2). Workflows that keep the regional load as a
        separate artefact (e.g. PyPSA-Eur's two-rule pipeline) call this directly.
        """
        raw = load_frame(self.loader, self.symbols["load_sector"])
        raw["year"] = raw["year"].astype(int)
        return convert_loads(raw, regions=self.remind_regions, unit_factor=1.0)

    def downscale_country_demand(self, regional: pd.DataFrame | None = None) -> pd.DataFrame:
        """Downscale REMIND regional demand to per-country annual demand by sector and year.

        Stage 2: restrict to the planning horizons, then split each REMIND region across its
        countries via the SSP population/GDP proxy (``disaggregate_demand_to_country``).
        Single-country regions pass through unchanged. ``regional`` lets a workflow pass in the
        Stage-1 frame it already produced (``build_regional_demand``); when omitted it is built
        from the GDX, so a single call still does the whole pipeline.
        """
        loads = self.build_regional_demand() if regional is None else regional
        if self.config.get("planning_horizons"):
            years = {int(y) for y in self.config["planning_horizons"]}
            loads = loads[loads["year"].isin(years)]
        return disaggregate_demand_to_country(
            loads,
            self.region_map,
            self.ssp_population,
            self.ssp_gdp,
            self.config["sector_weights"],
            set(self.config["countries"]),
        )

    def discount_rates(self, year: int) -> pd.Series:
        """Return the REMIND discount rate per region for ``year``, indexed by region.

        Reads the ``discount_rate`` symbol (REMIND ``p_r``) through the loader + central symbol
        config and filters to the configured REMIND regions. Raises if any configured region is
        missing a rate for the year.
        """
        p_r = load_frame(self.loader, self.symbols["discount_rate"])
        p_r = p_r[(p_r["year"].astype(str) == str(year)) & (p_r["region"].isin(self.remind_regions))]
        missing = set(self.remind_regions) - set(p_r["region"])
        if missing:
            raise ValueError(f"No REMIND discount rate for year {year}, regions: {sorted(missing)}")
        return p_r.set_index("region")["value"]

    def extract_cost_parameters(self, year: int) -> pd.DataFrame:
        """Extract REMIND cost parameters as long ``[region, reference, parameter, value, unit]``.

        Unit conversions are config-declared: ``load_frame``/``load_set`` apply the symbol's
        ``unit``/``to_unit`` (or per-row ``schema``) via the central ``rpycpl.units`` table. What
        remains here is REMIND *semantics* — which symbol holds what, the carrier filter, and the
        handful of genuine per-technology exceptions (``fnrs``/``tnrs`` efficiencies, ``peur``
        fuel, the storage cost label) that are tech facts, not unit math. Override only if a
        model's REMIND interface genuinely diverges.
        """
        y = str(year)
        load = lambda name: load_frame(self.loader, self.symbols[name])  # noqa: E731

        # investment: T$/TW→$/MW applied in load_frame; storage techs share the factor, only the
        # label differs ($/MWh vs $/MW).
        costs = load("cost_investment").query("year == @y").copy()
        costs["parameter"] = "investment"
        costs["unit"] = "USD/MW"
        costs.loc[costs["technology"].isin(["h2stor", "btstor"]), "unit"] = "USD/MWh"

        # tech_data: mixed-unit set (lifetime/FOM/VOM) — split + converted per the YAML schema.
        techd = load_set(self.loader, self.symbols["tech_data"])

        # CO2 intensity: Gt_C/TWa→t_CO2/MWh applied in load_frame; here just the carrier filter.
        co2i = load("emission_factor").query(
            "to_carrier == 'seel' & emission_type == 'co2' & year == @y"
        ).copy()
        co2i = co2i.assign(parameter="CO2 intensity", unit="t_CO2/MWh_th")

        # efficiency: p.u. (identity); the per-tech exceptions below are REMIND tech facts.
        eta = load("efficiency_conv").query("year == @y")
        dataeta = load("efficiency_data").query("year == @y")
        keys = set(zip(eta["region"], eta["technology"]))
        fallback = dataeta[
            ~pd.MultiIndex.from_arrays([dataeta["region"], dataeta["technology"]]).isin(keys)
        ]
        eff = pd.concat([eta, fallback]).assign(parameter="efficiency", unit="p.u.")
        eff.loc[eff["technology"].isin(["fnrs", "tnrs"]), "value"] *= HOURS_PER_YEAR / 1e6
        eff.loc[eff["technology"].isin(["fnrs", "tnrs"]), "unit"] = "MWh/g_U"

        # fuel: T$/TWa→$/MWh for all but the per-tech exception `peur` (already $/g_U).
        fuel = load("fuel_price").query("year == @y").copy()
        fuel["parameter"] = "fuel"
        fuel.loc[fuel["technology"] != "peur", "value"] *= unit_factor("T$/TWa", "$/MWh")
        fuel["unit"] = "USD/MWh_th"
        fuel.loc[fuel["technology"] == "peur", "unit"] = "USD/g_U"

        df = pd.concat([costs, techd, co2i, eff, fuel])[
            ["region", "technology", "parameter", "value", "unit"]
        ].rename(columns={"technology": "reference"})
        return df[df["region"].isin(self.remind_regions)]

    def build_costs(
        self, year: int, tech_map: pd.DataFrame, baseline: pd.DataFrame
    ) -> pd.DataFrame:
        """Assemble REMIND cost overrides onto the PyPSA baseline (shared mechanics).

        Extraction is delegated to ``extract_cost_parameters``; this maps to carriers, converts
        investment to input-capacity basis, and merges onto the baseline. Per-workflow
        annualisation (``capital_cost``) is done model-side (it depends on PyPSA's ``prepare_costs``).
        """
        remind_long = self.extract_cost_parameters(year)
        overrides = build_cost_overrides(tech_map, remind_long)
        overrides = convert_investment_to_input_capacity_basis(overrides)
        return merge_cost_overrides_into_baseline(baseline, overrides)
