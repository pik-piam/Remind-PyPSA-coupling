"""REMIND-GDX coupling adapter.

Subclass of ``CouplingAdapter`` that implements the two source-specific hooks for the GDX
backend: ``build_regional_demand`` (reads ``load_sector`` via a single ``load_frame`` call)
and ``extract_cost_parameters`` (reads investment/tech_data/efficiency/emission_factor/fuel
from GDX symbols, with the REMIND-specific per-tech quirks).

All other builders (``build_co2_prices``, ``discount_rates``, ``downscale_country_demand``)
are inherited from ``CouplingAdapter`` unchanged.
"""

from __future__ import annotations

import pandas as pd

from iampypsa.adapters.base import CouplingAdapter
from iampypsa.io.remind_symbols import load_frame, load_set
from iampypsa.transforms.loads import convert_loads
from iampypsa.units import HOURS_PER_YEAR, unit_factor


class RemindGdxAdapter(CouplingAdapter):
    """CouplingAdapter specialised for REMIND GDX output.

    Implements:
    - ``build_regional_demand``: reads ``load_sector`` GDX symbol (TWa→MWh via spec).
    - ``extract_cost_parameters``: reads all cost symbols from GDX, applies REMIND-GDX
      tech-facts (tnrs raw efficiency in TWa_elec/Mt_Ur, converted to MWh/g_U; peur raw
      fuel price in T$/Mt_Ur, relabelled USD/g_U; storage $/MWh label).
    """

    def build_regional_demand(self) -> pd.DataFrame:
        """Read REMIND regional sectoral demand as tidy ``[year, region, sector, value]`` (MWh/yr).

        Reads the ``load_sector`` symbol (TWa→MWh applied by ``load_frame``) and restricts
        to the configured REMIND regions. All available years are returned; the year filter to
        planning horizons happens in ``downscale_country_demand``.
        """
        raw = load_frame(self.loader, self.symbols["load_sector"])
        raw["year"] = raw["year"].astype(int)
        return convert_loads(raw, regions=self.model_regions, unit_factor=1.0)

    def extract_cost_parameters(self, year: int) -> pd.DataFrame:
        """Extract REMIND GDX cost parameters as long ``[region, reference, parameter, value, unit]``.

        Unit conversions are config-declared (applied by ``load_frame``/``load_set``). The
        REMIND-GDX–specific tech-facts encoded here are:
        - ``tnrs`` efficiency is raw ``TWa_elec/Mt_Ur`` (not p.u.) — converted to ``MWh/g_U``,
          then combined with the peur fuel price into a true ``USD/MWh_el`` fuel cost with
          ``tnrs`` efficiency reported as a genuine ``1.0`` p.u. (see ``_nuclear_fuel_cost``) —
          mirrors the IAMC path, and keeps ``Generator.efficiency`` physically sane downstream.
        - ``peur`` (uranium) fuel price is raw ``T$/Mt_Ur`` — numerically identical to
          ``USD/g_U``; other fuels get the T$/TWa→$/MWh conversion.
        - Storage techs (``h2stor``, ``btstor``) share the $/MW capex factor but are
          relabelled $/MWh.
        """
        y = str(year)
        load = lambda name: load_frame(self.loader, self.symbols[name])  # noqa: E731

        # Investment: T$/TW→$/MW applied in load_frame; storage techs relabelled $/MWh.
        costs = load("cost_investment").query("year == @y").copy()
        costs["parameter"] = "investment"
        costs["unit"] = "USD/MW"
        costs.loc[costs["technology"].isin(["h2stor", "btstor"]), "unit"] = "USD/MWh"

        # tech_data: mixed-unit set (lifetime/FOM/VOM) — split + converted per the YAML schema.
        techd = load_set(self.loader, self.symbols["tech_data"])

        # CO2 intensity: Gt_C/TWa→t_CO2/MWh applied in load_frame; carrier filter here.
        co2i = load("emission_factor").query(
            "to_carrier == 'seel' & emission_type == 'co2' & year == @y"
        ).copy()
        co2i = co2i.assign(parameter="CO2 intensity", unit="t_CO2/MWh_th")

        # Efficiency: p.u. (identity); per-tech exceptions below are REMIND GDX tech facts.
        eta = load("efficiency_conv").query("year == @y")
        dataeta = load("efficiency_data").query("year == @y")
        keys = set(zip(eta["region"], eta["technology"]))
        fallback = dataeta[
            ~pd.MultiIndex.from_arrays([dataeta["region"], dataeta["technology"]]).isin(keys)
        ]
        eff = pd.concat([eta, fallback]).assign(parameter="efficiency", unit="p.u.")
        # GDX nuclear efficiency is raw TWa_elec/Mt_Ur (mass basis, not thermal %). Convert to
        # MWh/g_U via the TWa->MWh, Mt->g factor (HOURS_PER_YEAR/1e6 = 8.76e9/1e12).
        eff.loc[eff["technology"].isin(["fnrs", "tnrs"]), "value"] *= HOURS_PER_YEAR / 1e6
        eff.loc[eff["technology"].isin(["fnrs", "tnrs"]), "unit"] = "MWh/g_U"

        # Fuel: T$/TWa→$/MWh for all except peur, whose raw unit is T$/Mt_Ur — numerically
        # identical to USD/g_U (T$=1e12 USD, Mt=1e12 g cancel exactly), so no scaling is applied.
        fuel = load("fuel_price").query("year == @y").copy()
        fuel["parameter"] = "fuel"
        fuel.loc[fuel["technology"] != "peur", "value"] *= unit_factor("T$/TWa", "$/MWh")
        fuel["unit"] = "USD/MWh_th"
        fuel.loc[fuel["technology"] == "peur", "unit"] = "USD/g_U"

        eff, fuel = self._nuclear_fuel_cost(eff, fuel)

        df = pd.concat([costs, techd, co2i, eff, fuel])[
            ["region", "technology", "parameter", "value", "unit"]
        ].rename(columns={"technology": "reference"})
        return df[df["region"].isin(self.model_regions)]

    @staticmethod
    def _nuclear_fuel_cost(eff: pd.DataFrame, fuel: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Convert tnrs/peur's mass-basis MWh/g_U, USD/g_U into a true USD/MWh_el fuel cost +
        1.0 p.u. efficiency (g_U cancels in the ratio); mirrors
        ``RemindIamcAdapter._nuclear_fuel_cost``. fnrs is left untouched (unattached to any
        network today).
        """
        eff = eff.copy()
        fuel = fuel.copy()

        tnrs_eta = eff.loc[eff["technology"] == "tnrs", ["region", "value"]].set_index("region")["value"]
        peur_price = fuel.loc[fuel["technology"] == "peur", ["region", "value"]].set_index("region")["value"]
        usd_per_mwh_el = peur_price / tnrs_eta

        is_tnrs = eff["technology"] == "tnrs"
        eff.loc[is_tnrs, "value"] = 1.0
        eff.loc[is_tnrs, "unit"] = "p.u."

        is_peur = fuel["technology"] == "peur"
        fuel.loc[is_peur, "value"] = fuel.loc[is_peur, "region"].map(usd_per_mwh_el)
        fuel.loc[is_peur, "unit"] = "USD/MWh_el"

        return eff, fuel
