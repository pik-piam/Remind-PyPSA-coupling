"""REMIND-IAMC / .mif coupling adapter.

Subclass of ``CouplingAdapter`` for the IAMC ``.mif`` backend. Implements the two
source-specific hooks:

- ``build_regional_demand``: derives sectoral electricity demand from SE|Electricity,
  transmission losses, and FE sector variables, applying an implicit T&D efficiency and
  computing the AC residual (all other loads).
- ``extract_cost_parameters``: reads per-parameter variable-sets from the mif, converts
  FOM from absolute (USD/MW/yr) to percentage of capex (%/yr), injects constant fallbacks
  for absent nuclear efficiency and CO2 intensity.

All other builders (``build_co2_prices``, ``discount_rates``, ``downscale_country_demand``)
are inherited from ``CouplingAdapter`` unchanged — they work for both backends via
spec-shape dispatch (``load_frame`` for ``symbol:``-shaped specs).
"""

from __future__ import annotations

import logging

import pandas as pd

from iampypsa.adapters.base import CouplingAdapter
from iampypsa.io.iamc import read_iamc
from iampypsa.io.remind_symbols import load_spec, load_variable_set
from iampypsa.units import unit_factor

logger = logging.getLogger(__name__)

#: EJ/yr → MWh conversion factor (1 EJ = 10^18 J; 1 MWh = 3.6e9 J).
_EJ_TO_MWH = unit_factor("EJ/yr", "MWh")

class RemindIamcAdapter(CouplingAdapter):
    """CouplingAdapter specialised for REMIND IAMC ``.mif`` output.

    Implements:
    - ``build_regional_demand``: FE sector rebasing via derived η_td + AC residual.
    - ``extract_cost_parameters``: loads per-parameter variable-sets, computes FOM%,
      injects nuclear efficiency and CO2 intensity fallbacks.
    """

    def build_regional_demand(self) -> pd.DataFrame:
        """Derive regional sectoral electricity demand from IAMC mif variables (MWh/yr).

        Variable names and sector token labels come from the symbol config.
        The algorithm applied on top is REMIND-specific:

        1. η_td = (SE − Losses) / SE  (derived T&D efficiency, replaces GDX pm_eta_conv).
        2. Each FE sector is rebased to the SE level: FE_sector_MWh / η_td.
        3. Electrolysis electricity demand (MWh_el) =
               (SE|Hydrogen|Electricity − SE|Input|Hydrogen|Electricity) / η_elec
           Both SE variables are in EJ H2; the difference is the net H2 from electricity
           destined for final-energy demand (not cycling back via fuel cells). Dividing
           by η_elec converts to the electricity consumed to produce it.
           This matches REMIND's p32_load_sector("elh2") on the GDX path exactly.
        4. AC = (SE − Losses) − Σ(rebased FE sectors) − electrolysis  (residual).
           Negative values are clamped to 0 with a warning.

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
        """Extract REMIND mif cost parameters as ``[region, reference, parameter, value, unit]``.

        Reads five per-parameter variable-sets declared in the IAMC symbol config
        (cost_investment, tech_lifetime, cost_omf, cost_omv, efficiency, fuel_price),
        queries to ``year``, and:

        - Computes FOM%/yr = absolute FOM (USD/MW/yr) / capex (USD/MW) × 100, because the
          mif reports absolute FOM whereas PyPSA-Eur uses percent-of-capex.
        - Injects a constant nuclear efficiency fallback (0.33) across all regions.
        - Injects per-fuel CO2 intensity fallbacks (t_CO2/MWh_th) for all fossil techs,
          spread across all (region, year) combinations present in the data.

        Battery cost tokens are intentionally omitted (their mif values are full-system
        costs per kW_power and are not comparable to PyPSA-Eur's separate inverter+storage
        parametrisation; those techs fall back to the PyPSA-Eur baseline).
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

        # --- efficiency (p.u.) — nuclear (tnrs) fallback injected automatically by load_variable_set ---
        eff = load("efficiency")
        eff["parameter"] = "efficiency"
        eff["unit"] = "p.u."
        yr_reg = eff[["year", "region"]].drop_duplicates() if not eff.empty else pd.DataFrame(
            columns=["year", "region"]
        )

        # --- fuel price (USD/MWh_th) ---
        fuel = load("fuel_price")
        fuel["parameter"] = "fuel"
        fuel["unit"] = "USD/MWh_th"
        if currency_factor != 1.0:
            fuel["value"] *= currency_factor

        # --- CO2 intensity fallback for all fossil techs — values and tech→fuel map from config ---
        co2_grid = capex[["year", "region"]].drop_duplicates() if not capex.empty else yr_reg
        co2i = self._co2_intensity_fallback(co2_grid)

        # --- assemble ---
        frames = [capex, lifetime, fom_pct, vom, eff, fuel, co2i]
        keep = ["region", "reference", "parameter", "value", "unit"]
        df = pd.concat(
            [f[keep] for f in frames if set(keep).issubset(f.columns)],
            ignore_index=True,
        )
        return df[df["region"].isin(set(self.model_regions))]

    # -- extract_cost_parameters helpers ------------------------------------

    @staticmethod
    def _compute_fom_pct(capex: pd.DataFrame, fom_abs: pd.DataFrame) -> pd.DataFrame:
        """FOM %/yr = absolute FOM (USD/MW/yr) / capex (USD/MW) × 100, joined on tech/region/year."""
        fom_pct = capex[["year", "region", "reference", "value"]].merge(
            fom_abs[["year", "region", "reference", "value"]],
            on=["year", "region", "reference"],
            suffixes=("_cap", "_fom"),
        )
        fom_pct["value"] = fom_pct["value_fom"] / fom_pct["value_cap"] * 100
        fom_pct["parameter"] = "FOM"
        fom_pct["unit"] = "%/yr"
        return fom_pct

    def _co2_intensity_fallback(self, grid: pd.DataFrame) -> pd.DataFrame:
        """Per-fuel IPCC Tier 1 CO2 intensity (t_CO2/MWh_th) for every fossil tech over ``grid``."""
        ef_spec = self.symbols["emission_factor"]
        tech_fuel = ef_spec["tech_fuel_map"]
        co2_by_fuel = {k: v["value"] for k, v in ef_spec["fallback"].items()}
        co2_rows = []
        for tech, fuel_key in tech_fuel.items():
            tf = grid.copy()
            tf["reference"] = tech
            tf["parameter"] = "CO2 intensity"
            tf["value"] = co2_by_fuel.get(fuel_key, 0.0)
            tf["unit"] = "t_CO2/MWh_th"
            co2_rows.append(tf)
        if not co2_rows:
            return pd.DataFrame(
                columns=["year", "region", "reference", "parameter", "value", "unit"]
            )
        co2i = pd.concat(co2_rows, ignore_index=True)
        logger.warning(
            "CO2 emission intensity absent from mif; using IPCC Tier 1 fallback values "
            "(%d tech×region×year rows). Refresh from a GDX-derived emission_factor source "
            "if exact REMIND intensities are needed.",
            len(co2i),
        )
        return co2i
