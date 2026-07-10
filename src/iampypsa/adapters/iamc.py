"""REMIND-IAMC / .mif coupling adapter.

Subclass of ``CouplingAdapter`` for the IAMC ``.mif`` backend. Implements the two
source-specific hooks:

- ``build_regional_demand``: derives sectoral electricity demand from SE|Electricity,
  transmission losses, and FE sector variables, applying an implicit T&D efficiency and
  computing the AC residual (all other loads).
- ``extract_cost_parameters``: reads per-parameter variable-sets from the mif, converts
  FOM from absolute (USD/MW/yr) to percentage of capex (%/yr), and computes nuclear's fuel
  cost/efficiency from REMIND's uranium mass-basis price and conversion-factor variables.

All other builders (``build_co2_prices``, ``discount_rates``, ``downscale_country_demand``)
are inherited from ``CouplingAdapter`` unchanged — they work for both backends via
spec-shape dispatch (``load_frame`` for ``symbol:``-shaped specs).
"""

from __future__ import annotations

import logging

import pandas as pd

from iampypsa.adapters.base import CouplingAdapter
from iampypsa.io.iamc import read_iamc
from iampypsa.io.remind_symbols import load_frame, load_spec, load_variable_set, rename_technologies
from iampypsa.transforms.costs import broadcast_fuel_prices
from iampypsa.units import unit_factor

logger = logging.getLogger(__name__)

#: EJ/yr → MWh conversion factor (1 EJ = 10^18 J; 1 MWh = 3.6e9 J).
_EJ_TO_MWH = unit_factor("EJ/yr", "MWh")

class RemindIamcAdapter(CouplingAdapter):
    """CouplingAdapter specialised for REMIND IAMC ``.mif`` output.

    Implements:
    - ``build_regional_demand``: FE sector rebasing via derived η_td + AC residual.
    - ``extract_cost_parameters``: loads per-parameter variable-sets, computes FOM%,
      derives nuclear fuel cost/efficiency from mass-basis REMIND variables.
    """

    def build_regional_demand(self) -> pd.DataFrame:
        """Derive regional sectoral electricity demand from IAMC mif variables (MWh/yr).

        REMIND-specific algorithm:
        1. η_td = (SE − Losses) / SE  (derived T&D efficiency, replaces GDX pm_eta_conv).
        2. Each FE sector is rebased to the SE level: FE_sector_MWh / η_td.
        3. Electrolysis demand (MWh_el) = (SE|Hydrogen|Electricity − SE|Input|Hydrogen|
           Electricity) / η_elec — matches REMIND's p32_load_sector("elh2") on the GDX path.
        4. AC = (SE − Losses) − Σ(rebased FE) − electrolysis  (residual, clamped to ≥ 0).

        Returns ``[year, region, sector, value, unit]`` matching the GDX path.
        """
        fe_df = load_spec(self.loader, self.symbols["demand_fe_sectors"])
        fe_df = fe_df[fe_df["region"].isin(set(self.model_regions))]

        se_var      = self.symbols["demand_se_electricity"]["symbol"]
        losses_var  = self.symbols["demand_transmission_losses"]["symbol"]
        h2_prod_var = self.symbols["demand_h2_from_electricity"]["symbol"]
        h2_turb_var = self.symbols["demand_h2_to_turbines"]["symbol"]
        eta_var     = self.symbols["demand_electrolysis_efficiency"]["symbol"]

        scalar_df = read_iamc(
            self.loader.source,
            variables=[se_var, losses_var, h2_prod_var, h2_turb_var, eta_var],
        )
        scalar_df = scalar_df[scalar_df["region"].isin(set(self.model_regions))]

        rows = []
        for (region, year), grp in scalar_df.groupby(["region", "year"]):
            get = self._group_getter(grp)

            se = get(se_var)
            losses = get(losses_var)
            eta_td = self._td_efficiency(se, losses)

            fe_slice = fe_df[(fe_df["region"] == region) & (fe_df["year"] == year)]
            fe_rows, rebased_sum_mwh = self._rebase_fe_sectors(fe_slice, eta_td, region, year)
            rows.extend(fe_rows)

            eta_elec = get(eta_var) / 100.0  # mif reports in %
            elec_h2_mwh = self._electrolysis_demand_mwh(
                get(h2_prod_var), get(h2_turb_var), eta_elec, region, year
            )
            rows.append({
                "year": year, "region": region, "sector": "electrolysis",
                "value": elec_h2_mwh, "unit": "MWh",
            })

            ac_mwh = self._ac_residual(se, losses, rebased_sum_mwh, elec_h2_mwh, region, year)
            rows.append({
                "year": year, "region": region, "sector": "AC",
                "value": ac_mwh, "unit": "MWh",
            })

        return (
            pd.DataFrame(rows)
            .sort_values(["year", "region", "sector"])
            .reset_index(drop=True)
        )

    # -- build_regional_demand helpers --------------------------------------

    @staticmethod
    def _group_getter(grp: pd.DataFrame):
        """Return a ``get(variable) -> float`` summing that variable's values in ``grp``."""
        def get(v: str) -> float:
            vals = grp.loc[grp["variable"] == v, "value"]
            return float(vals.sum()) if not vals.empty else 0.0
        return get

    @staticmethod
    def _td_efficiency(se: float, losses: float) -> float:
        """Derived T&D efficiency η_td = (SE − Losses) / SE (1.0 when SE is non-positive)."""
        return (se - losses) / se if se > 0 else 1.0

    @staticmethod
    def _rebase_fe_sectors(
        fe_slice: pd.DataFrame, eta_td: float, region: str, year: int
    ) -> tuple[list[dict], float]:
        """Rebase each FE sector to the SE level (FE / η_td); return (rows, summed MWh)."""
        rows = []
        rebased_sum_mwh = 0.0
        for _, fe_row in fe_slice.iterrows():
            se_val_mwh = fe_row["value"] / eta_td if eta_td > 0 else fe_row["value"]
            rebased_sum_mwh += se_val_mwh
            rows.append({
                "year": year, "region": region, "sector": fe_row["sector"],
                "value": se_val_mwh, "unit": "MWh",
            })
        return rows, rebased_sum_mwh

    @staticmethod
    def _electrolysis_demand_mwh(
        h2_prod_ej: float, h2_turb_ej: float, eta_elec: float, region: str, year: int
    ) -> float:
        """Net H2 for FE demand (EJ H2) ÷ η_elec → EJ electricity → MWh (0 if η_elec ≤ 0)."""
        if eta_elec > 0:
            return (h2_prod_ej - h2_turb_ej) / eta_elec * _EJ_TO_MWH
        logger.warning("Zero electrolysis efficiency for region=%s year=%s; skipping.", region, year)
        return 0.0

    @staticmethod
    def _ac_residual(
        se: float, losses: float, rebased_sum_mwh: float, elec_h2_mwh: float,
        region: str, year: int,
    ) -> float:
        """AC residual: (SE − Losses) − Σ(rebased FE) − electrolysis, in MWh (clamped ≥ 0)."""
        ac_mwh = (se - losses) * _EJ_TO_MWH - rebased_sum_mwh - elec_h2_mwh
        if ac_mwh < 0:
            logger.warning(
                "Negative AC residual for region=%s year=%s: %.4f MWh — clamped to 0.",
                region, year, ac_mwh,
            )
            ac_mwh = 0.0
        return ac_mwh

    def extract_cost_parameters(self, year: int) -> pd.DataFrame:
        """Extract REMIND mif cost parameters as ``[region, technology, parameter, value, unit]``.

        Reads six per-parameter variable-sets (investment, lifetime, FOM, VOM, efficiency,
        fuel, CO2 intensity), computes FOM%/yr from absolute FOM ÷ capex, and injects
        nuclear's fuel cost/efficiency from REMIND's uranium mass-basis variables (see
        ``_nuclear_fuel_cost``). Battery cost tokens are omitted — their mif values aren't
        comparable to PyPSA-Eur's separate inverter+storage parametrisation, so those techs
        fall back to the PyPSA-Eur baseline.
        """
        y = str(year)
        currency_factor: float = self.config.get("currency_factor", 1.0)

        def load(name: str) -> pd.DataFrame:
            df = load_variable_set(self.loader, self.symbols[name])
            return df[df["year"].astype(str) == y].copy()

        # --- investment (USD/MW) ---
        capex = load("cost_investment")
        capex["parameter"] = "investment"
        capex["unit"] = "USD/MW"
        if currency_factor != 1.0:
            capex["value"] *= currency_factor

        # --- lifetime (yr) ---
        lifetime = load("tech_lifetime")
        lifetime["parameter"] = "lifetime"
        lifetime["unit"] = "yr"

        # --- FOM: compute %/yr = absolute / capex × 100 ---
        fom_abs = load("cost_omf")
        if currency_factor != 1.0:
            fom_abs["value"] *= currency_factor
        fom_pct = self._compute_fom_pct(capex, fom_abs)

        # --- VOM (USD/MWh) ---
        vom = load("cost_omv")
        vom["parameter"] = "VOM"
        vom["unit"] = "USD/MWh"
        if currency_factor != 1.0:
            vom["value"] *= currency_factor

        # --- efficiency (p.u.) ---
        eff = load("efficiency")
        eff["parameter"] = "efficiency"
        eff["unit"] = "p.u."

        # --- fuel price (USD/MWh_th) ---
        fuel = load("fuel_price")
        fuel["parameter"] = "fuel"
        fuel["unit"] = "USD/MWh_th"
        if currency_factor != 1.0:
            fuel["value"] *= currency_factor

        # --- nuclear: fuel cost (USD/MWh_el) + efficiency (1.0 p.u.), computed from the
        # uranium mass-basis price/conversion-factor variables (mass unit cancels in the
        # ratio); see _nuclear_fuel_cost.
        nuclear_fuel, nuclear_eff = self._nuclear_fuel_cost(year)
        if currency_factor != 1.0:
            nuclear_fuel["value"] *= currency_factor
        fuel = pd.concat([fuel, nuclear_fuel], ignore_index=True)
        eff = pd.concat([eff, nuclear_eff], ignore_index=True)

        # --- CO2 intensity (t_CO2/MWh_th) — biomass techs fall back to 0.0 (carbon-neutral) ---
        co2i = load("emission_factor")
        co2i["parameter"] = "CO2 intensity"
        co2i["unit"] = "t_CO2/MWh_th"

        # --- assemble ---
        frames = [capex, lifetime, fom_pct, vom, eff, fuel, co2i]
        keep = ["region", "technology", "parameter", "value", "unit"]
        df = pd.concat(
            [f[keep] for f in frames if set(keep).issubset(f.columns)],
            ignore_index=True,
        )
        # Output boundary (mirrors the GDX adapter): rename is a no-op here — mif labels are
        # already canonical — then per-fuel price rows become one `fuel` row per technology.
        df = rename_technologies(df, self.symbols.get("technology_names"))
        df = broadcast_fuel_prices(df, self.symbols.get("tech_fuel_map"))
        return df[df["region"].isin(set(self.model_regions))].reset_index(drop=True)

    # -- extract_cost_parameters helpers ------------------------------------

    @staticmethod
    def _compute_fom_pct(capex: pd.DataFrame, fom_abs: pd.DataFrame) -> pd.DataFrame:
        """FOM %/yr = absolute FOM (USD/MW/yr) / capex (USD/MW) × 100, joined on tech/region/year."""
        fom_pct = capex[["year", "region", "technology", "value"]].merge(
            fom_abs[["year", "region", "technology", "value"]],
            on=["year", "region", "technology"],
            suffixes=("_cap", "_fom"),
        )
        fom_pct["value"] = fom_pct["value_fom"] / fom_pct["value_cap"] * 100
        fom_pct["parameter"] = "FOM"
        fom_pct["unit"] = "%/yr"
        return fom_pct

    def _nuclear_fuel_cost(self, year: int) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Compute nuclear's fuel cost (USD/MWh_el) and efficiency (1.0 p.u.) for ``year``.

        REMIND reports nuclear on a uranium-mass basis (price ÷ conversion factor cancels
        kg_Ur, leaving USD/GJ_el, converted to USD/MWh); efficiency is reported as a genuine
        1.0 p.u. so downstream consumers (marginal_cost = fuel / efficiency,
        Generator.efficiency) stay consistent.
        """
        y = str(year)
        conversion = load_frame(self.loader, self.symbols["nuclear_conversion_factor"])
        price = load_frame(self.loader, self.symbols["nuclear_price"])
        conversion = conversion[conversion["year"].astype(str) == y]
        price = price[price["year"].astype(str) == y]

        merged = price.merge(
            conversion[["region", "value"]].rename(columns={"value": "conversion_factor"}),
            on="region",
        )
        merged["value"] = (
            merged["value"] / merged["conversion_factor"]
            * unit_factor("US$2017/GJ", "USD/MWh")
        )
        merged["parameter"] = "fuel"
        merged["unit"] = "USD/MWh_el"

        eff = merged[["year", "region"]].copy()
        eff["technology"] = "nuclear"
        eff["parameter"] = "efficiency"
        eff["value"] = 1.0
        eff["unit"] = "p.u."

        # Tag the computed fuel price with the canonical fuel name; broadcast_fuel_prices
        # then folds it into nuclear's own `fuel` row via the config's tech_fuel_map
        # (nuclear: uranium), like every other fuel.
        merged["technology"] = "uranium"
        fuel = merged[["year", "region", "technology", "parameter", "value", "unit"]]

        return fuel, eff
