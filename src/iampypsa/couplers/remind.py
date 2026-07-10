"""REMIND coupling backends: GDX and IAMC (``.mif``).

Two ``Coupler`` subclasses implementing the source-specific hooks
(``build_regional_demand`` and ``extract_cost_parameters``) for REMIND output:

- ``RemindGdxCoupler``  — GDX backend (reads ``load_sector``/cost symbols directly).
- ``RemindIamcCoupler`` — IAMC ``.mif`` backend (derives demand via T&D efficiency + AC
  residual; reads per-parameter variable-sets, converts FOM to %/capex, injects nuclear
  efficiency and CO2-intensity fallbacks).

All other builders (``build_co2_prices``, ``discount_rates``, ``downscale_country_demand``)
are inherited from ``Coupler`` unchanged — they work for both backends via
spec-shape dispatch.
"""

from __future__ import annotations

import logging

import pandas as pd

from iampypsa.couplers.base import Coupler
from iampypsa.io.iamc import read_iamc
from iampypsa.io.remind_symbols import load_frame, load_set, load_spec, load_variable_set
from iampypsa.transforms.loads import convert_loads
from iampypsa.units import HOURS_PER_YEAR, unit_factor

logger = logging.getLogger(__name__)

#: EJ/yr → MWh conversion factor (1 EJ = 10^18 J; 1 MWh = 3.6e9 J).
_EJ_TO_MWH = unit_factor("EJ/yr", "MWh")


class RemindGdxCoupler(Coupler):
    """Coupler specialised for REMIND GDX output.

    Implements:
    - ``build_regional_demand``: reads ``load_sector`` GDX symbol (TWa→MWh via spec).
    - ``extract_cost_parameters``: reads all cost symbols from GDX, applies REMIND-GDX
      tech-facts (fnrs/tnrs MWh/g_U efficiency, peur $/g_U fuel, storage $/MWh label).
    """

    def build_regional_demand(self) -> pd.DataFrame:
        """Read REMIND regional sectoral demand as tidy ``[year, region, sector, value]`` (MWh/yr).

        Reads ``demand_fe_sectors`` when present (fallback: ``load_sector``), with
        TWa→MWh conversion applied by ``load_frame``, and restricts
        to the configured REMIND regions. All available years are returned; the year filter to
        planning horizons happens in ``downscale_country_demand``.
        """
        key = "demand_fe_sectors" if "demand_fe_sectors" in self.symbols else "load_sector"
        raw = load_frame(self.loader, self.symbols[key])
        raw["year"] = raw["year"].astype(int)
        return convert_loads(raw, regions=self.model_regions, unit_factor=1.0)

    def extract_cost_parameters(self, year: int) -> pd.DataFrame:
        """Extract REMIND GDX cost parameters as long
        ``[region, reference, parameter, value, unit]``.

        Unit conversions are config-declared (applied by ``load_frame``/``load_set``). The
        REMIND-GDX-specific tech-facts encoded here are:
        - ``fnrs``/``tnrs`` efficiency is in MWh/g_U (not p.u.); kept as-is and labelled
          accordingly - the cost model uses it together with the peur $/g_U fuel price.
        - ``peur`` (uranium) fuel price is already in $/g_U in the GDX; other fuels get the
          T$/TWa->$/MWh conversion applied.
        - Storage techs (``h2stor``, ``btstor``) share the $/MW capex factor but are
          relabelled $/MWh.
        """
        year_str = str(year)
        load = lambda name: load_frame(self.loader, self.symbols[name])  # noqa: E731

        # Investment: T$/TW->$/MW applied in load_frame; storage techs relabelled $/MWh.
        costs = load("cost_investment")
        costs = costs.loc[costs["year"].astype(str) == year_str].copy()
        costs["parameter"] = "investment"
        costs["unit"] = "USD/MW"
        costs.loc[costs["technology"].isin(["h2stor", "btstor"]), "unit"] = "USD/MWh"

        # tech_data: mixed-unit set (lifetime/FOM/VOM) - split + converted per YAML schema.
        techd = load_set(self.loader, self.symbols["tech_data"])

        # CO2 intensity: Gt_C/TWa->t_CO2/MWh applied in load_frame; carrier filter here.
        co2i = load("emission_factor")
        co2i = co2i.loc[
            (co2i["to_carrier"] == "seel")
            & (co2i["emission_type"] == "co2")
            & (co2i["year"].astype(str) == year_str)
        ].copy()
        co2i = co2i.assign(parameter="CO2 intensity", unit="t_CO2/MWh_th")

        # Efficiency: p.u. (identity); per-tech exceptions below are REMIND GDX tech facts.
        eta = load("efficiency_conv")
        eta = eta.loc[eta["year"].astype(str) == year_str]
        dataeta = load("efficiency_data")
        dataeta = dataeta.loc[dataeta["year"].astype(str) == year_str]
        keys = set(zip(eta["region"], eta["technology"]))
        fallback = dataeta[
            ~pd.MultiIndex.from_arrays([dataeta["region"], dataeta["technology"]]).isin(keys)
        ]
        eff = pd.concat([eta, fallback]).assign(parameter="efficiency", unit="p.u.")
        # GDX nuclear efficiency is in MWh/g_U (mass basis), not thermal %; keep the unit.
        eff.loc[eff["technology"].isin(["fnrs", "tnrs"]), "value"] *= HOURS_PER_YEAR / 1e6
        eff.loc[eff["technology"].isin(["fnrs", "tnrs"]), "unit"] = "MWh/g_U"

        # Fuel: T$/TWa->$/MWh for all except peur (already $/g_U in GDX).
        fuel = load("fuel_price")
        fuel = fuel.loc[fuel["year"].astype(str) == year_str].copy()
        fuel["parameter"] = "fuel"
        fuel.loc[fuel["technology"] != "peur", "value"] *= unit_factor("T$/TWa", "$/MWh")
        fuel["unit"] = "USD/MWh_th"
        fuel.loc[fuel["technology"] == "peur", "unit"] = "USD/g_U"

        df = pd.concat([costs, techd, co2i, eff, fuel])[
            ["region", "technology", "parameter", "value", "unit"]
        ].rename(columns={"technology": "reference"})
        return df[df["region"].isin(self.model_regions)]


class RemindIamcCoupler(Coupler):
    """Coupler specialised for REMIND IAMC ``.mif`` output.

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
          2. Electricity FE sectors are rebased to the SE level: FE_sector_MWh / η_td.
              ``demand_h2`` is treated as a hydrogen-demand quantity and is not rebased.
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
            fe_rows, rebased_sum_mwh, has_fe_h2 = self._rebase_fe_sectors(
                fe_slice, eta_td, region, year
            )
            rows.extend(fe_rows)

            h2_demand_mwh = self._net_h2_demand_mwh(
                get(h2_prod_var), get(h2_turb_var), region, year
            )
            if not has_fe_h2:
                rows.append(
                    {
                        "year": year,
                        "region": region,
                        "sector": "demand_h2",
                        "value": h2_demand_mwh,
                        "unit": "MWh",
                    }
                )

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
    ) -> tuple[list[dict], float, bool]:
        """Return FE rows, rebased-electricity sum, and whether FE provided ``demand_h2``.

        Electricity FE sectors are rebased by ``η_td`` and contribute to the AC residual
        subtraction. ``demand_h2`` is treated as hydrogen demand and passed through
        unchanged, i.e. it is not rebased and does not enter the AC residual sum.
        """
        rows = []
        rebased_sum_mwh = 0.0
        has_fe_h2 = False
        for _, fe_row in fe_slice.iterrows():
            sector = str(fe_row["sector"])
            if sector == "demand_h2":
                has_fe_h2 = True
                rows.append(
                    {
                        "year": year,
                        "region": region,
                        "sector": sector,
                        "value": fe_row["value"],
                        "unit": "MWh",
                    }
                )
                continue

            se_val_mwh = fe_row["value"] / eta_td if eta_td > 0 else fe_row["value"]
            rebased_sum_mwh += se_val_mwh
            rows.append({
                "year": year, "region": region, "sector": sector,
                "value": se_val_mwh, "unit": "MWh",
            })
        return rows, rebased_sum_mwh, has_fe_h2

    @staticmethod
    def _net_h2_demand_mwh(h2_prod_ej: float, h2_turb_ej: float, region: str, year: int) -> float:
        """Net hydrogen demand (MWh_H2) from electricity-route hydrogen balances.
        Seasonal storage is a pypsa-decision for elec supply and not a load -> discarded

        Computes ``(SE|Hydrogen|Electricity - SE|Input|Hydrogen|Electricity)`` and
        converts EJ/yr to MWh/yr. Negative values are clamped to zero.
        """
        net_h2_mwh = (h2_prod_ej - h2_turb_ej) * _EJ_TO_MWH
        if net_h2_mwh < 0:
            logger.warning(
                "Negative net H2 demand for region=%s year=%s: %.4f MWh - clamped to 0.",
                region,
                year,
                net_h2_mwh,
            )
            return 0.0
        return net_h2_mwh
        

    @staticmethod
    def _electrolysis_demand_mwh(
        h2_prod_ej: float, h2_turb_ej: float, eta_elec: float, region: str, year: int
    ) -> float:
        """Electrolyser electricity demand caused by green H2 IAM demand.

        electrolysis_el_dem = (Demand_H2_tot - H2_seasonal_storage)[EJ] ÷ η_elec x EJ_to_MWh 
        (0 if η_elec ≤ 0).
        Seasonal storage is a pypsa-decision to meet elec demand and not an H2 load -> discarded here

        Args:
            h2_prod_ej: SE|Hydrogen|Electricity (EJ H2)
            h2_turb_ej: SE|Input|Hydrogen|Electricity (EJ H2)
            eta_elec: Electrolysis efficiency (p.u.)
            region: Region name (for logging)
            year: Planning year (for logging)
        Returns:
            float: Net Electrolysis demand to produce green H2 in MWh excluding IAM seasonal storage.
        """
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
