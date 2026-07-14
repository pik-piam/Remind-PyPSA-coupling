"""REMIND coupling backends: GDX and IAMC (``.mif``).

Two ``Coupler`` subclasses implementing the source-specific hooks
(``build_regional_demand`` and ``extract_cost_parameters``) for REMIND output:

- ``RemindGdxCoupler``  — GDX backend (reads ``load_sector``/cost symbols directly).
- ``RemindIamcCoupler`` — IAMC ``.mif`` backend (derives demand via T&D efficiency + AC
  residual; reads per-parameter variable-sets, converts FOM to %/capex, derives nuclear
  fuel cost/efficiency from mass-basis REMIND variables).

All other builders (``build_co2_prices``, ``discount_rates``, ``downscale_country_demand``)
are inherited from ``Coupler`` unchanged — they work for both backends via
spec-shape dispatch.
"""

import logging

import pandas as pd
import country_converter as coco
import os.path 
from iampypsa.couplers.base import Coupler
from iampypsa.io.iamc import read_iamc
from iampypsa.io.remind_symbols import PathLike, load_frame, load_set, load_spec, load_variable_set, rename_technologies
from iampypsa.transforms.costs import broadcast_fuel_prices, annotate_cost_rows
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
      tech-facts (tnrs/peur nuclear mass-basis → USD/MWh_el, storage $/MWh label,
      GAMS-dropped zeros filled).
    """

    def build_regional_demand(self) -> pd.DataFrame:
        """Read REMIND regional sectoral demand as tidy ``[year, region, sector, value]`` (MWh/yr).

        Reads ``demand_fe_sectors`` (``p32_load_sector``), with TWa→MWh conversion applied by
        ``load_frame``, and restricts to the configured REMIND regions. All available years are
        returned; the year filter to planning horizons happens in ``downscale_country_demand``.
        """
        raw = load_frame(self.loader, self.symbols["demand_fe_sectors"])
        raw["year"] = raw["year"].astype(int)
        return convert_loads(raw, regions=self.model_regions, unit_factor=1.0)

    def extract_cost_parameters(self, year: int) -> pd.DataFrame:
        """Extract REMIND GDX cost parameters as long
        ``[region, technology, parameter, value, unit]``.

        Unit conversions are config-declared (applied by ``load_frame``/``load_set``). The
        REMIND-GDX-specific tech-facts encoded here are:
        - ``tnrs`` (nuclear) efficiency is mass-basis (TWa_elec/Mt_Ur); combined with ``peur``'s
          $/g_U fuel price into a true USD/MWh_el cost + 1.0 p.u. efficiency (see
          ``_nuclear_fuel_cost``).
        - Storage techs (``h2stor``, ``btstor``) share the $/MW capex factor but are
          relabelled $/MWh.
        - GDX/GAMS drops explicit zeros, so entries missing for a modeled technology are
          true zeros (filled via ``_fill_missing_with_zero``).
        """
        year_str = str(year)
        load = lambda name: load_frame(self.loader, self.symbols[name])  # noqa: E731

        # Investment: T$/TW->$/MW applied in load_frame; storage techs relabelled $/MWh.
        costs = load("cost_investment")
        costs = costs.loc[costs["year"].astype(str) == year_str].copy()
        costs = annotate_cost_rows(costs, parameter="investment", unit="USD/MW")
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
        co2i = annotate_cost_rows(co2i, parameter="CO2 intensity", unit="t_CO2/MWh_th")

        # GDX/GAMS drops explicit zeros, so missing entries for modeled technologies are true zeros.
        modeled_techs = costs[["region", "technology"]].drop_duplicates()
        co2i = self._fill_missing_with_zero(co2i, modeled_techs, "CO2 intensity", "t_CO2/MWh_th")
        vom = techd[techd["parameter"] == "VOM"]
        vom_filled = self._fill_missing_with_zero(vom, modeled_techs, "VOM", "$/MWh")
        techd = pd.concat([techd[techd["parameter"] != "VOM"], vom_filled], ignore_index=True)

        # Efficiency
        dataeta = load("efficiency_data")
        dataeta = dataeta.loc[dataeta["year"].astype(str) == year_str]
        eta = load("efficiency_conv")
        eta = eta.loc[eta["year"].astype(str) == year_str]
        keys = set(zip(dataeta["region"], dataeta["technology"]))
        fallback = eta[
            ~pd.MultiIndex.from_arrays([eta["region"], eta["technology"]]).isin(keys)
        ]
        eff = annotate_cost_rows(pd.concat([dataeta, fallback]), parameter="efficiency", unit="p.u.")
        # GDX nuclear efficiency is raw TWa_elec/Mt_Ur (mass basis, not thermal %). Convert to
        # MWh/g_U via the TWa->MWh, Mt->g factor (HOURS_PER_YEAR/1e6 = 8.76e9/1e12).
        eff.loc[eff["technology"] == "tnrs", "value"] *= HOURS_PER_YEAR / 1e6
        eff.loc[eff["technology"] == "tnrs", "unit"] = "MWh/g_U"

        # Fuel: T$/TWa->$/MWh for all except peur (already $/g_U in GDX).
        fuel = load("fuel_price")
        fuel = fuel.loc[fuel["year"].astype(str) == year_str].copy()
        fuel.loc[fuel["technology"] != "peur", "value"] *= unit_factor("T$/TWa", "$/MWh")
        fuel = annotate_cost_rows(fuel, parameter="fuel", unit="USD/MWh_th")
        fuel.loc[fuel["technology"] == "peur", "unit"] = "USD/g_U"

        eff, fuel = self._nuclear_fuel_cost(eff, fuel)

        df = pd.concat([costs, techd, co2i, eff, fuel])[
            ["region", "technology", "parameter", "value", "unit"]
        ]
        # Output boundary: raw REMIND tokens -> canonical vocabulary
        names = self.symbols.get("technology_names", {})
        df = rename_technologies(df, names)
        df = broadcast_fuel_prices(df, self._tech_fuel_map_from_pe2se())
        df = df[df["technology"].isin(set(names.values()))]
        return df[df["region"].isin(self.model_regions)].reset_index(drop=True)

    def _tech_fuel_map_from_pe2se(self) -> dict[str, str]:
        """Build the canonical ``technology -> fuel`` map from the GDX ``pe2se`` set.
        """
        names = self.symbols.get("technology_names", {})
        pe2se = load_frame(self.loader, self.symbols["pe2se"])
        seel = pe2se[pe2se["all_enty_1"] == "seel"]
        return {
            names.get(row["all_te_2"], row["all_te_2"]): names[row["all_enty_0"]]
            for _, row in seel.iterrows()
            if row["all_enty_0"] in names  # priced PE carrier (has a technology_names entry)
        }

    @staticmethod
    def _fill_missing_with_zero(
        sparse: pd.DataFrame, modeled_techs: pd.DataFrame, parameter: str, unit: str,
    ) -> pd.DataFrame:
        """Reindex against modeled technologies (from investment data), filling gaps with 0 —
        GDX/GAMS drops explicit zeros, so a missing entry there is a true zero, not a gap."""
        merged = modeled_techs.merge(
            sparse[["region", "technology", "value"]], on=["region", "technology"], how="left",
        )
        merged["value"] = merged["value"].fillna(0.0)
        merged["parameter"] = parameter
        merged["unit"] = unit
        return merged

    @staticmethod
    def _nuclear_fuel_cost(eff: pd.DataFrame, fuel: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Correct tnrs/peur's rows in-place: mass-basis MWh/g_U, USD/g_U -> USD/MWh_el fuel cost
        + 1.0 p.u. efficiency.
        """
        eff = eff.copy()
        fuel = fuel.copy()

        tnrs_eta = eff.loc[eff["technology"] == "tnrs", ["region", "value"]].set_index("region")["value"]
        peur_price = fuel.loc[fuel["technology"] == "peur", ["region", "value"]].set_index("region")["value"]
        usd_per_mwh_el = peur_price / tnrs_eta  # g_U mass unit cancels

        is_tnrs = eff["technology"] == "tnrs"
        eff.loc[is_tnrs, "value"] = 1.0
        eff.loc[is_tnrs, "unit"] = "p.u."

        is_peur = fuel["technology"] == "peur"
        # Categorical region dtype survives .map() as Categorical floats; cast to plain float
        # before assigning into the float64 column, or .loc hits a pandas AssertionError.
        mapped = fuel.loc[is_peur, "region"].map(usd_per_mwh_el).astype(float)
        fuel.loc[is_peur, "value"] = mapped
        fuel.loc[is_peur, "unit"] = "USD/MWh_el"

        return eff, fuel


class RemindIamcCoupler(Coupler):
    """Coupler specialised for REMIND IAMC ``.mif`` output.

    Implements:
    - ``build_regional_demand``: FE sector rebasing via derived η_td + AC residual.
    - ``extract_cost_parameters``: loads per-parameter variable-sets, computes FOM%,
      derives nuclear fuel cost/efficiency from mass-basis REMIND variables.
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
            fe_rows, rebased_sum_mwh, has_se_h2 = self._rebase_fe_sectors(
                fe_slice, eta_td, region, year
            )
            rows.extend(fe_rows)

            h2_demand_mwh = self._net_h2_demand_mwh(
                get(h2_prod_var), get(h2_turb_var), region, year
            )
            if not has_se_h2:
                rows.append(
                    {
                        "year": year,
                        "region": region,
                        "sector": "demand_h2",
                        "value": h2_demand_mwh,
                        "unit": "MWh_H2",
                    }
                )

            eta_elec = get(eta_var) / 100.0  # mif reports in %
            elec_h2_mwh = h2_demand_mwh/eta_elec

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
        has_se_h2 = False
        for _, fe_row in fe_slice.iterrows():
            sector = str(fe_row["sector"])
            if sector == "demand_h2":
                has_se_h2 = True
                rows.append(
                    {
                        "year": year,
                        "region": region,
                        "sector": sector,
                        "value": fe_row["value"],
                        "unit": "MWh_H2",
                    }
                )
                continue

            se_val_mwh = fe_row["value"] / eta_td if eta_td > 0 else fe_row["value"]
            rebased_sum_mwh += se_val_mwh
            rows.append({
                "year": year, "region": region, "sector": sector,
                "value": se_val_mwh, "unit": "MWh",
            })
        return rows, rebased_sum_mwh, has_se_h2

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

        Reads six per-parameter variable-sets declared in the IAMC symbol config
        (cost_investment, tech_lifetime, cost_omf, cost_omv, efficiency, fuel_price,
        emission_factor), queries to ``year``, and:

        - Computes FOM%/yr = absolute FOM (USD/MW/yr) / capex (USD/MW) × 100, because the
          mif reports absolute FOM whereas PyPSA-Eur uses percent-of-capex.
        - Derives nuclear's fuel cost/efficiency from mass-basis price/conversion-factor
          variables (see ``_nuclear_fuel_cost``) — not a fallback, real per-region REMIND data.
        - Reads real per-tech CO2 intensity (t_CO2/MWh_th); biomass techs without a mif
          variable fall back to 0.0 (carbon-neutral), declared in the symbol config.

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
        capex = annotate_cost_rows(load("cost_investment"), parameter="investment", unit="USD/MW", factor=currency_factor)

        # --- lifetime (yr) ---
        lifetime = annotate_cost_rows(load("tech_lifetime"), parameter="lifetime", unit="yr")

        # --- FOM: compute %/yr = absolute / capex × 100 ---
        fom_abs = load("cost_omf")
        if currency_factor != 1.0:
            fom_abs["value"] *= currency_factor
        fom_pct = self._compute_fom_pct(capex, fom_abs)

        # --- VOM (USD/MWh) ---
        vom = annotate_cost_rows(load("cost_omv"), parameter="VOM", unit="USD/MWh", factor=currency_factor)

        # --- efficiency (p.u.) ---
        eff = annotate_cost_rows(load("efficiency"), parameter="efficiency", unit="p.u.")

        # --- fuel price (USD/MWh_th) ---
        fuel = annotate_cost_rows(load("fuel_price"), parameter="fuel", unit="USD/MWh_th", factor=currency_factor)

        # --- nuclear: fuel cost (USD/MWh_el) + efficiency (1.0 p.u.), computed from the
        # uranium mass-basis price/conversion-factor variables (mass unit cancels in the
        # ratio); see _nuclear_fuel_cost.
        nuclear_fuel, nuclear_eff = self._nuclear_fuel_cost(year)
        if currency_factor != 1.0:
            nuclear_fuel["value"] *= currency_factor
        fuel = pd.concat([fuel, nuclear_fuel], ignore_index=True)
        eff = pd.concat([eff, nuclear_eff], ignore_index=True)

        # --- CO2 intensity (t_CO2/MWh_th) — biomass techs fall back to 0.0 (carbon-neutral) ---
        co2i = annotate_cost_rows(load("emission_factor"), parameter="CO2 intensity", unit="t_CO2/MWh_th")

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

        REMIND reports nuclear on a uranium-mass basis (price ÷ conversion factor cancels the
        mass unit); efficiency is reported as a genuine 1.0 p.u. so downstream consumers
        (marginal_cost = fuel / efficiency, Generator.efficiency) stay consistent.
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
        # kg_Ur mass unit cancels; the remaining GJ->MWh mismatch needs unit_factor.
        merged["value"] = (
            merged["value"] / merged["conversion_factor"] * unit_factor("US$2017/GJ", "USD/MWh")
        )
        merged["technology"] = "uranium"
        fuel = annotate_cost_rows(merged, parameter="fuel", unit="USD/MWh_el")
        # Tag the computed fuel price with the canonical fuel name; broadcast_fuel_prices
        # then folds it into nuclear's own `fuel` row via the config's tech_fuel_map
        # (nuclear: uranium), like every other fuel.
        fuel = fuel[["year", "region", "technology", "parameter", "value", "unit"]]

        eff = fuel[["year", "region"]].copy()
        eff["technology"] = "nuclear"
        eff = annotate_cost_rows(eff, parameter="efficiency", unit="p.u.")
        eff["value"] = 1.0

        return fuel, eff

def read_region_map(
    source="country",
    target="model_region",
    file_path: str | PathLike | None = None,
    flatten: bool = False,
) -> dict:
    """Read the REMIND region↔country mapping as ``{source: [target, ...]}``.

    Reads the ``;``-separated mapping CSV (columns ``RegionCode``/``CountryCode``), converts
    ISO3 country codes to ISO2, and adds Kosovo (XK → NES). Pass ``source``/``target`` as
    ``"model_region"`` or ``"country"`` to select the groupby direction.
    """
    if file_path is None:
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        file_path = os.path.join(base_path, "data/remind", "regions.csv")
    region_mapping = pd.read_csv(file_path, sep=";").rename(columns={"RegionCode": "model_region"})
    region_mapping["country"] = coco.convert(names=region_mapping["CountryCode"], to="ISO2")
    region_mapping = region_mapping[["country", "model_region"]]

    # Kosovo: PyPSA-Eur uses "XK" (not recognised by country_converter); part of NES.
    region_mapping = pd.concat(
        [region_mapping, pd.DataFrame({"country": ["XK"], "model_region": ["NES"]})]
    ).reset_index(drop=True)

    grouped = region_mapping.groupby(source)[target].apply("unique").apply(list)
    if flatten:
        grouped = grouped.apply(lambda x: x[0])
    return grouped.to_dict()

