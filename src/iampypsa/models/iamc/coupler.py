"""Coupler for the generic IAMC model-intercomparison exchange file.

Both hooks are driven purely by ``quantities.yaml``: ``demand_fe_sectors`` names the demand
variables and their sector labels, and ``cost_parameters:`` names one quantity spec per
techno-economic parameter. There are no per-technology or per-IAM special cases here — an IAM
whose file needs derived quantities (residuals, mass-basis fuel costs, an efficiency chain)
gets its own ``models/<iam>/`` instead, as REMIND does.

The exchange profile this expects: demand already at the level PyPSA consumes (no transmission
losses to unwind), FOM already as percent-of-capex per year, and one variable per technology.
"""

import pandas as pd

from iampypsa.coupler import Coupler
from iampypsa.quantities.load import load_quantity
from iampypsa.transforms.costs import annotate_cost_rows

#: Columns of the long cost frame the coupling contract requires.
COST_COLUMNS = ["region", "technology", "parameter", "value", "unit"]


class IamcCoupler(Coupler):
    """Couple a generic IAMC exchange file, config-driven end to end."""

    def build_regional_demand(self) -> pd.DataFrame:
        """Read regional sectoral demand as ``[year, region, sector, value, unit]`` (MWh/yr).

        Sums rows sharing a key as a guard against an unexpected extra source dimension. All
        available years are returned; the planning-horizon filter happens downstream in
        ``downscale_country_demand``.
        """
        df = load_quantity(self.loader, self.quantities["demand_fe_sectors"])
        df = df[df["region"].isin(set(self.model_regions))].copy()
        df["year"] = df["year"].astype(int)
        return (
            df.groupby(["year", "region", "sector", "unit"], as_index=False, observed=True)["value"]
            .sum()
            .sort_values(["year", "region", "sector"])
            .reset_index(drop=True)
        )

    def build_cost_parameters(self, year: int) -> pd.DataFrame:
        """Assemble cost rows as long ``[region, technology, parameter, value, unit]``.

        One quantity spec per parameter, named by the config's ``cost_parameters:`` block. Units
        come from each spec's ``to_unit:``, stamped at the load seam; the currency factor and the
        canonical vocabulary are applied by ``Coupler.finalise_cost_parameters``.
        """
        frames = []
        for parameter, name in self.quantities["cost_parameters"].items():
            df = load_quantity(self.loader, self.quantities[name])
            df = df[df["year"].astype(int) == int(year)]
            frames.append(annotate_cost_rows(df, parameter=parameter))

        return pd.concat([f[COST_COLUMNS] for f in frames], ignore_index=True)
