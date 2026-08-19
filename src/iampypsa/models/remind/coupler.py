"""REMIND coupling backends: GDX and IAMC (``.mif``).

Two ``Coupler`` subclasses implementing the source-specific hooks
(``build_regional_demand`` and ``extract_cost_parameters``) for REMIND output:

- ``RemindGdxCoupler``  — GDX backend (reads ``load_sector``/cost symbols directly).
- ``RemindIamcCoupler`` — IAMC ``.mif`` backend (derives demand via T&D efficiency + AC
  residual; reads per-parameter variable-sets, converts FOM to %/capex, derives nuclear
  fuel cost/efficiency from mass-basis REMIND variables).

All other builders (``build_co2_prices``, ``build_discount_rates``, ``downscale_country_demand``)
are inherited from ``Coupler`` unchanged — they work for both backends via
spec-shape dispatch.
"""

import importlib.resources
import logging
from os import PathLike

import pandas as pd
import country_converter as coco

from iampypsa.coupler import Coupler
from iampypsa.quantities.load import load_quantity, rename_technologies
from iampypsa.transforms.costs import broadcast_fuel_prices, annotate_cost_rows, apply_currency_factor
from iampypsa.units import HOURS_PER_YEAR, unit_factor


logger = logging.getLogger(__name__)


class RemindGdxCoupler(Coupler):
    """Coupler specialised for REMIND GDX output.

    Implements:
    - ``build_regional_demand``: reads ``load_sector`` GDX symbol (TWa→MWh via spec).
    - ``extract_cost_parameters``: reads all cost symbols from GDX, applies REMIND-GDX
      tech-facts (tnrs/peur nuclear mass-basis → USD/MWh_el, storage USD/MWh label,
      GAMS-dropped zeros filled).
    """

    def build_regional_demand(self) -> pd.DataFrame:
        """Read REMIND regional sectoral demand as long ``[year, region, sector, value]`` (MWh/yr).

        Reads ``demand_fe_sectors`` (``p32_load_sector``), with TWa→MWh conversion applied by
        ``load_quantity``, and restricts to the configured REMIND regions. All available years are
        returned; the year filter to planning horizons happens in ``downscale_country_demand``.
        Sums rows sharing a key as a guard against an unexpected extra source dimension.
        """
        raw = load_quantity(self.loader, self.quantities["demand_fe_sectors"])
        raw["year"] = raw["year"].astype(int)
        df = raw[["year", "region", "sector", "value"]].copy()
        df["unit"] = "MWh_el"
        df = df[df["region"].isin(set(self.model_regions))]
        return (
            df.groupby(["year", "region", "sector", "unit"], as_index=False, observed=True)["value"]
            .sum()
            .sort_values(["year", "region", "sector"])
            .reset_index(drop=True)
        )

    def extract_cost_parameters(self, year: int) -> pd.DataFrame:
        """Extract REMIND GDX cost parameters as long
        ``[region, technology, parameter, value, unit]``.

        Unit conversions are config-declared (applied at the load seam). The
        REMIND-GDX-specific tech-facts encoded here are:
        - ``tnrs`` (nuclear) efficiency is mass-basis (TWa_elec/Mt_Ur); combined with ``peur``'s
          USD/g_U fuel price into a true USD/MWh_el cost + 1.0 p.u. efficiency (see
          ``_nuclear_fuel_cost``).
        - Storage techs (``h2stor``, ``btstor``) share the USD/MW capex factor but are
          relabelled USD/MWh.
        - GDX/GAMS drops explicit zeros, so entries missing for a modeled technology are
          true zeros (filled via ``_fill_missing_with_zero``).
        - ``currency_factor`` (config) scales ``investment``/``VOM``/``fuel`` (REMIND reports
          USD) into the PyPSA baseline's currency.
        """
        year_str = str(year)
        load = lambda name: load_quantity(self.loader, self.quantities[name])  # noqa: E731

        # Investment: unit conversion applied at the load seam. Storage capex is per MWh of store,
        # not per MW of converter, but shares the same GDX symbol — so relabel only.
        costs = load("cost_investment")
        costs = costs.loc[costs["year"].astype(str) == year_str].copy()
        costs = annotate_cost_rows(costs, parameter="investment")
        costs.loc[costs["technology"].isin(["h2stor", "btstor"]), "unit"] = "USD/MWh"

        # tech_data: mixed-unit indexed symbol (lifetime/FOM/VOM) - split + converted per schema.
        techd = load("tech_data")

        # CO2 intensity: unit conversion and the carrier/emission-type slice applied at the load seam.
        co2i = load("emission_factor")
        co2i = co2i.loc[co2i["year"].astype(str) == year_str].copy()
        co2i = annotate_cost_rows(co2i, parameter="CO2 intensity")

        # GDX/GAMS drops explicit zeros, so missing entries for modeled technologies are true zeros.
        modeled_techs = costs[["region", "technology"]].drop_duplicates()
        co2i = self._fill_missing_with_zero(co2i, modeled_techs, "CO2 intensity")
        vom = techd[techd["parameter"] == "VOM"]
        vom_filled = self._fill_missing_with_zero(vom, modeled_techs, "VOM")
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
        eff = annotate_cost_rows(pd.concat([dataeta, fallback]), parameter="efficiency")
        # GDX nuclear efficiency is raw TWa_elec/Mt_Ur (mass basis, not thermal %), so the spec's
        # p.u. does not hold for it. Convert to MWh/g_U via TWa->MWh, Mt->g (8.76e9/1e12).
        eff.loc[eff["technology"] == "tnrs", "value"] *= HOURS_PER_YEAR / 1e6
        eff.loc[eff["technology"] == "tnrs", "unit"] = "MWh/g_U"

        # Fuel: the spec cannot declare to_unit here because peur is already USD/g_U in GDX while
        # every other carrier is TUSD/TWa — so the conversion and the thermal-basis label are set
        # per-carrier below rather than at the load seam.
        fuel = load("fuel_price")
        fuel = fuel.loc[fuel["year"].astype(str) == year_str].copy()
        fuel.loc[fuel["technology"] != "peur", "value"] *= unit_factor("TUSD/TWa", "USD/MWh")
        fuel = annotate_cost_rows(fuel, parameter="fuel", unit="USD/MWh_th")
        fuel.loc[fuel["technology"] == "peur", "unit"] = "USD/g_U"

        eff, fuel = self._nuclear_fuel_cost(eff, fuel)

        df = pd.concat([costs, techd, co2i, eff, fuel])[
            ["region", "technology", "parameter", "value", "unit"]
        ]
        df = apply_currency_factor(df, self.config.get("currency_factor", 1.0))
        # Output boundary: raw REMIND tokens -> canonical vocabulary
        names = self.quantities.get("technology_names", {})
        df = rename_technologies(df, names)
        df = broadcast_fuel_prices(df, self._tech_fuel_map_from_pe2se())
        df = df[df["technology"].isin(set(names.values()))]
        return df[df["region"].isin(self.model_regions)].reset_index(drop=True)

    def _tech_fuel_map_from_pe2se(self) -> dict[str, str]:
        """Build the canonical ``technology -> fuel`` map from the GDX ``pe2se`` set.
        """
        names = self.quantities.get("technology_names", {})
        seel = load_quantity(self.loader, self.quantities["pe2se"])
        return {
            names.get(row["all_te_2"], row["all_te_2"]): names[row["all_enty_0"]]
            for _, row in seel.iterrows()
            if row["all_enty_0"] in names  # priced PE carrier (has a technology_names entry)
        }

    @staticmethod
    def _fill_missing_with_zero(
        sparse: pd.DataFrame, modeled_techs: pd.DataFrame, parameter: str,
    ) -> pd.DataFrame:
        """Reindex against modeled technologies (from investment data), filling gaps with 0 —
        GDX/GAMS drops explicit zeros, so a missing entry there is a true zero, not a gap.

        The unit is carried over from ``sparse`` so the filled rows keep whatever the symbol
        spec declared.

        Args:
            sparse: The loaded frame, with explicit zeros already dropped by GAMS.
            modeled_techs: The ``[region, technology]`` pairs to reindex against.
            parameter: Canonical parameter name for the resulting rows.
        """
        units = sparse["unit"].dropna().unique() if "unit" in sparse.columns else []
        merged = modeled_techs.merge(
            sparse[["region", "technology", "value"]], on=["region", "technology"], how="left",
        )
        merged["value"] = merged["value"].fillna(0.0)
        merged["parameter"] = parameter
        merged["unit"] = units[0] if len(units) else None
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
    - ``build_regional_demand``: FE sector SE-conversion via derived η_td + AC residual.
    - ``extract_cost_parameters``: loads per-parameter variable-sets, computes FOM%,
      derives nuclear fuel cost/efficiency from mass-basis REMIND variables.
    """

    def build_regional_demand(self) -> pd.DataFrame:
        """Derive regional sectoral electricity demand from IAMC mif variables (MWh/yr).

        Variable names and sector token labels come from the quantity config; unit conversion is
        config-driven (``to_unit:`` on ``demand_energy_balance``/``demand_fe_sectors``/
        ``demand_electrolysis_efficiency``). The algorithm applied on top is REMIND-specific:

        1. η_td = (SE − Losses) / SE  (derived T&D efficiency, replaces GDX pm_eta_conv).
          2. Electricity FE sectors are converted to the SE level: FE_sector_MWh / η_td.
              ``demand_h2`` is treated as a hydrogen-demand quantity and is not converted.
        3. Electrolysis electricity demand (MWh_el) =
               (SE|Hydrogen|Electricity − SE|Input|Hydrogen|Electricity) / η_elec
           The difference is the net H2 from electricity destined for final-energy demand
           (not cycling back via fuel cells). Dividing by η_elec converts to the electricity
           consumed to produce it.
        4. AC = (SE − Losses) − Σ(SE-converted FE sectors) − electrolysis  (residual).
           Negative values are clamped to 0 with a warning.

        Returns ``[year, region, sector, value, unit]`` matching the GDX path.
        """
        fe_df = load_quantity(self.loader, self.quantities["demand_fe_sectors"])
        fe_df = fe_df[fe_df["region"].isin(set(self.model_regions))]

        energy_balance = load_quantity(self.loader, self.quantities["demand_energy_balance"])
        energy_balance = energy_balance[energy_balance["region"].isin(set(self.model_regions))]

        eta_df = load_quantity(self.loader, self.quantities["demand_electrolysis_efficiency"])
        eta_df = eta_df[eta_df["region"].isin(set(self.model_regions))]
        eta_lookup = eta_df.groupby(["region", "year"])["value"].sum()

        def row(region: str, year: int, sector: str, value: float, unit: str) -> dict:
            return {"year": year, "region": region, "sector": sector, "value": value, "unit": unit}

        rows = []
        for (region, year), grp in energy_balance.groupby(["region", "year"]):
            get = self._group_getter(grp, col="quantity")
            se, losses = get("se"), get("losses")
            eta_td = self._td_efficiency(se, losses)

            fe_slice = fe_df[(fe_df["region"] == region) & (fe_df["year"] == year)]
            fe_rows, se_sum_mwh, has_se_h2 = self._convert_fe_sectors_to_se(fe_slice, eta_td, region, year)
            rows.extend(fe_rows)

            h2_demand_mwh = self._net_h2_demand_mwh(get("h2_prod"), get("h2_turb"), region, year)
            if not has_se_h2:
                rows.append(row(region, year, "demand_h2", h2_demand_mwh, "MWh_H2"))

            eta_elec = eta_lookup.get((region, year), 0.0)
            elec_h2_mwh = h2_demand_mwh / eta_elec
            rows.append(row(region, year, "electrolysis", elec_h2_mwh, "MWh"))

            ac_mwh = self._ac_residual(se, losses, se_sum_mwh, elec_h2_mwh, region, year)
            rows.append(row(region, year, "AC", ac_mwh, "MWh"))

        return (
            pd.DataFrame(rows)
            .sort_values(["year", "region", "sector"])
            .reset_index(drop=True)
        )

    # -- build_regional_demand helpers --------------------------------------

    @staticmethod
    def _group_getter(grp: pd.DataFrame, col: str = "variable"):
        """Return a ``get(key) -> float`` summing ``grp[col] == key`` rows' ``value``."""
        def get(key: str) -> float:
            vals = grp.loc[grp[col] == key, "value"]
            return float(vals.sum()) if not vals.empty else 0.0
        return get

    @staticmethod
    def _td_efficiency(se: float, losses: float) -> float:
        """Derived T&D efficiency η_td = (SE − Losses) / SE (1.0 when SE is non-positive)."""
        return (se - losses) / se if se > 0 else 1.0

    @staticmethod
    def _convert_fe_sectors_to_se(
        fe_slice: pd.DataFrame, eta_td: float, region: str, year: int
    ) -> tuple[list[dict], float, bool]:
        """Return FE rows, SE-converted electricity sum, and whether FE provided ``demand_h2``.

        Electricity FE sectors are converted to SE by dividing by ``η_td`` and contribute to
        the AC residual subtraction. ``demand_h2`` is treated as hydrogen demand and passed
        through unchanged, i.e. it is not converted and does not enter the AC residual sum.
        """
        rows = []
        se_sum_mwh = 0.0
        has_se_h2 = False
        for _, fe_row in fe_slice.iterrows():
            sector = str(fe_row["sector"])
            if sector == "demand_h2":
                has_se_h2 = True
                rows.append({
                    "year": year, "region": region, "sector": sector,
                    "value": fe_row["value"], "unit": "MWh_H2",
                })
                continue

            se_val_mwh = fe_row["value"] / eta_td if eta_td > 0 else fe_row["value"]
            se_sum_mwh += se_val_mwh
            rows.append({
                "year": year, "region": region, "sector": sector,
                "value": se_val_mwh, "unit": "MWh",
            })
        return rows, se_sum_mwh, has_se_h2

    @staticmethod
    def _net_h2_demand_mwh(h2_prod_mwh: float, h2_turb_mwh: float, region: str, year: int) -> float:
        """Net hydrogen demand (MWh_H2) from electricity-route hydrogen balances.
        Seasonal storage is a pypsa-decision for elec supply and not a load -> discarded

        Computes ``(SE|Hydrogen|Electricity - SE|Input|Hydrogen|Electricity)``, already in
        MWh/yr (config-converted). Negative values are clamped to zero.
        """
        net_h2_mwh = h2_prod_mwh - h2_turb_mwh
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
        se: float, losses: float, se_sum_mwh: float, elec_h2_mwh: float,
        region: str, year: int,
    ) -> float:
        """AC residual: (SE − Losses) − Σ(SE-converted FE) − electrolysis, in MWh (clamped ≥ 0)."""
        ac_mwh = (se - losses) - se_sum_mwh - elec_h2_mwh
        if ac_mwh < 0:
            logger.warning(
                "Negative AC residual for region=%s year=%s: %.4f MWh — clamped to 0.",
                region, year, ac_mwh,
            )
            ac_mwh = 0.0
        return ac_mwh

    def extract_cost_parameters(self, year: int) -> pd.DataFrame:
        """Extract REMIND mif cost parameters as ``[region, technology, parameter, value, unit]``.

        - Computes FOM%/yr = absolute FOM (USD/MW/yr) / capex (USD/MW) × 100, because the
          mif reports absolute FOM whereas PyPSA uses percent-of-capex.
        - Derives nuclear's fuel cost/efficiency from mass-basis price/conversion-factor
          variables (see ``_nuclear_fuel_cost``).
        - ``currency_factor`` (config) scales ``investment``/``VOM``/``fuel`` (REMIND reports
          USD) into the PyPSA baseline's currency.
        """
        y = str(year)
        currency_factor: float = self.config.get("currency_factor", 1.0)

        def load(name: str) -> pd.DataFrame:
            df = load_quantity(self.loader, self.quantities[name])
            return df[df["year"].astype(str) == y].copy()

        # Units below come from each spec's to_unit:, stamped at the load seam.
        capex = annotate_cost_rows(load("cost_investment"), parameter="investment")
        lifetime = annotate_cost_rows(load("tech_lifetime"), parameter="lifetime")
        vom = annotate_cost_rows(load("cost_omv"), parameter="VOM")
        eff = annotate_cost_rows(load("efficiency"), parameter="efficiency")

        # --- FOM: compute %/yr = absolute / capex × 100 ---
        fom_abs = load("cost_omf")
        fom_pct = self._compute_fom_pct(capex, fom_abs)

        # Fuel prices are per unit of thermal input; the mif's unit string does not say so, and
        # downstream marginal_cost = fuel / efficiency depends on that basis.
        fuel = annotate_cost_rows(load("fuel_price"), parameter="fuel", unit="USD/MWh_th")

        # --- nuclear: fuel cost (USD/MWh_el) + efficiency (1.0 p.u.), computed from the
        # uranium mass-basis price/conversion-factor variables (mass unit cancels in the
        # ratio); see _nuclear_fuel_cost.
        nuclear_fuel, nuclear_eff = self._nuclear_fuel_cost(year)
        fuel = pd.concat([fuel, nuclear_fuel], ignore_index=True)
        eff = pd.concat([eff, nuclear_eff], ignore_index=True)

        # --- CO2 intensity — biomass techs fall back to 0.0 (carbon-neutral) ---
        co2i = annotate_cost_rows(load("emission_factor"), parameter="CO2 intensity")

        # --- assemble ---
        frames = [capex, lifetime, fom_pct, vom, eff, fuel, co2i]
        keep = ["region", "technology", "parameter", "value", "unit"]
        df = pd.concat(
            [f[keep] for f in frames if set(keep).issubset(f.columns)],
            ignore_index=True,
        )
        df = apply_currency_factor(df, currency_factor)
        # Output boundary (mirrors RemindGdxCoupler): rename is a no-op here — mif labels are
        # already canonical — then per-fuel price rows become one `fuel` row per technology.
        df = rename_technologies(df, self.quantities.get("technology_names"))
        df = broadcast_fuel_prices(df, self.quantities.get("tech_fuel_map"))
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
        conversion = load_quantity(self.loader, self.quantities["nuclear_conversion_factor"])
        price = load_quantity(self.loader, self.quantities["nuclear_price"])
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
        # importlib.resources, not a path walk from __file__: the CSV is package data and must
        # resolve the same way for an editable checkout and an installed wheel.
        file_path = importlib.resources.files("iampypsa.models.remind").joinpath("regions.csv")
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


def build_capacity_reporting_technologies(quantities: dict) -> set[str]:
    """Return every canonical technology the ``capacity`` spec reports installed capacity for.

    Reads the spec's ``variables:``/``derived:`` tokens (``hydro`` is included there), so it is
    meaningful only for a backend whose capacity spec has that shape — the IAMC one.
    """
    cap_spec = quantities["capacity"]
    return set(cap_spec.get("variables", {}).values()) | set(cap_spec.get("derived", {}))

