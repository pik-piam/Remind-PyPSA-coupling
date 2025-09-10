"""
# Extract, Transform, Load (ETL) operations for REMIND (pre-invetment) generation capacities

The aim is to translate the REMIND pre-investment capacities into pypsa brownfield capacities.
PyPSA workflows already come with their own bronwfield data (e.g. from powerplantmatching) assigned
 to nodes/clusters. This capacity needs to be adjusted to the REMIND capacities.

## Harmonisation of REMIND and PypSA Capacities
In case the REMIND capacities are smaller than the pypsa brownfield capacities,
     the pypsa capacities are scaled down by tech.

In case the REMIND capacities are larger, the pypsa brownfield capacities are kept and an
     additional paid-off component is added to the pypsa model as a max (paid-off ie free)
     capacity constraint. The constraint is REMIND REGION wide so that pypsa
     determines the optimal location of the REMIND-built capacity.

## Workflow integration
The constraints and data are exported as files made available to the pypsa workflow.
- use the ETL transformations `convert_remind_capacities` to prpeare the data
- use `build_tech_map` to creat tech_groups from the technoeconomic `mapping.csv`
- merge the pypsa capacities data with the tech_groups
- idem for the converted remind capacities data
- use the `harmonize_capacities` ETL to harmonize the capacities
- finally use the `calc_paidoff_capacity` ETL to calculate the paid-off capacities
"""

import pandas as pd
import logging

# from warnings import deprecated

logger = logging.getLogger()
pd.set_option("display.max_columns", 5)
pd.set_option("display.precision", 2)


def scale_down_capacities(to_scale: pd.DataFrame, reference: pd.DataFrame) -> pd.DataFrame:
    """
    Scale down the target (existing pypsa) capacities to not exceed the refernce (remind)
        capacities by tech group. The target capacities can have a higher spatial resolution.
        This function can be used to harmonize the capacities between REMIND and PyPSA.
        Scaling is done by groups of techs, which allows n:1 mapping of remind to pypsa techs.

    Args:
        to_scale (pd.DataFrame): DataFrame with the target (pypsa) capacities for a single year.
        reference (pd.DataFrame): DataFrame with the ref (remind) capacities by tech group.
    Returns:
        pd.DataFrame: DataFrame with capacities clipped to the reference for each tech group.
    Example:
        remind_caps = pd.DataFrame({"technology": ["wind", "hydro"], "capacity": [300, 200]})
        data = {'hydro': {('Capacity', 'node1'): 240, ('Capacity', 'node2'): 360},
                'wind': {('Capacity', 'node1'): 20, ('Capacity', 'node2'): 120}})
        pypsa_caps = pd.DataFrame.from_dict(d, orient="index") # poweplantmatching
        scaled_caps = scale_down_capacities(pypsa_caps, remind_caps)
        >> {'hydro': {('Capacity', 'node1'): 120, ('Capacity', 'node2'): 180}, # scaled down
                'wind': {('Capacity', 'node1'): 20, ('Capacity', 'node2'): 120}}) # untouched
    """
    if reference.year.nunique() > 1:
        raise ValueError("The reference capacities should be for a single year")
    # group the target & ref capacities by tech group
    group_totals_ref = reference.groupby(["tech_group"]).capacity.sum()
    to_scale.loc[:, "group_fraction"] = (
        to_scale.groupby("tech_group").Capacity.transform(lambda x: x / x.sum()).values
    )

    missing = to_scale.query("tech_group == ''")[["Fueltype", "Tech"]].drop_duplicates()
    if not missing.empty:
        logger.warning(
            "Some technologies are not assigned to a tech group. "
            f"Missing from tech groups: {missing}"
        )
        to_scale = to_scale.query("tech_group != ''")

    # set missing tech groups to zero in reference
    not_in_ref = set(to_scale.tech_group.unique()).difference(set(group_totals_ref.index))
    if not_in_ref:
        group_totals_ref = pd.concat([group_totals_ref, pd.Series(0, index=not_in_ref)])

    # clip the capacities so they don't exceed the existing (excess will be added as paid-off)
    to_scale.rename(columns={"Capacity": "original_capacity"}, inplace=True)
    allocated_caps = to_scale.groupby("tech_group")["original_capacity"].sum()
    group_totals_ref = group_totals_ref.clip(upper=allocated_caps).fillna(group_totals_ref)

    # perform the scaling (normalised target capacities * ref capacities)
    logger.info("applying scaling to capacities")
    to_scale.loc[:, "Capacity"] = to_scale.groupby("tech_group").group_fraction.transform(
        lambda x: x * group_totals_ref[x.name]
    )

    return to_scale


def calc_paidoff_capacity(
    remind_capacities: pd.DataFrame, harmonized_pypsa_caps: pd.DataFrame
) -> pd.DataFrame:
    """
    Calculate the aditional paid off capacity available to pypsa from REMIND investment decisions.
    The paid off capacity is the difference between the REMIND capacities and the harmonized
    pypsa capacities. The paid off capacity is available to pypsa as a zero-capex tech.

    Args:
        remind_capacities (pd.DataFrame): DataFrame with remind capacities in MW.
        harmonized_pypsa_caps (pd.DataFrame): harmonized pypsa capacities by year (capped to REMIND cap)
    Returns:
        pd.DataFrame: DataFrame with the available paid off capacity by tech group.
    """

    pypsa_caps = (
        harmonized_pypsa_caps.groupby(["remind_year", "tech_group"])
        .apply(
            lambda x: pd.Series(
                {
                    "capacity": x.Capacity.sum(),
                    "techs": ",".join(x.Tech.unique()),
                }
            )
        )
        .reset_index()
    )
    pypsa_caps["year"] = pypsa_caps.remind_year.astype(int)
    remind_caps = remind_capacities.groupby(["tech_group", "year"]).capacity.sum().reset_index()
    merged = pd.merge(
        remind_caps,
        pypsa_caps,
        how="left",
        on=["year", "tech_group"],
        suffixes=("_remind", "_pypsa"),
    ).fillna(0)
    # TODO check for nans and raise warnings
    merged["paid_off"] = merged.capacity_remind - merged.capacity_pypsa
    if (merged.paid_off < -1e-6).any():
        raise ValueError(
            "Found negative Paid off capacities. This indicates that harmonized PyPSA capacities"
            " exceed the REMIND capacities. Please check the harmonization step."
        )

    return (
        merged.groupby(["tech_group", "year"])
        .paid_off.sum()
        .clip(lower=0)
        .reset_index()
        .rename(columns={"paid_off": "Capacity"})
    )


def calc_paidoff_capacity_multiyear(
    remind_capacities: pd.DataFrame, harmonized_pypsa_caps: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """
    Calculate the aditional paid off capacity available to pypsa from REMIND investment decisions.
    The paid off capacity is the difference between the REMIND capacities and the harmonized
    pypsa capacities. The paid off capacity is available to pypsa as a zero-capex tech.

    Args:
        remind_capacities (pd.DataFrame): DataFrame with remind capacities in MW.
        harmonized_pypsa_caps (dict[str, pd.DataFrame]): Dictionary with harmonized
            pypsa capacities by year (capped to REMIND cap)
    Returns:
        pd.DataFrame: DataFrame with the available paid off capacity by tech group.
    """
    if harmonized_pypsa_caps == {}:
        raise ValueError("Harmonized PyPSA capacities must be provided.")

    # merge all years of harmonized capacities into a single DataFrame
    def grp(df, yr):
        return df.groupby("tech_group").apply(
            lambda x: pd.Series(
                {"capacity": x.Capacity.sum(), "year": yr, "techs": ",".join(x.Tech)}
            )
        )

    grouped_by_tech = [grp(df, yr) for yr, df in harmonized_pypsa_caps.items() if not df.empty]
    if not grouped_by_tech:
        raise ValueError("No harmonized capacities provided for any year.")

    pypsa_caps = pd.concat(grouped_by_tech)
    pypsa_caps.year = pypsa_caps.year.astype(int)
    remind_caps = remind_capacities.groupby(["tech_group", "year"]).capacity.sum().reset_index()
    merged = pd.merge(
        remind_caps,
        pypsa_caps,
        how="left",
        on=["year", "tech_group"],
        suffixes=("_remind", "_pypsa"),
    ).fillna(0)
    # TODO check for nans and raise warnings
    merged["paid_off"] = merged.capacity_remind - merged.capacity_pypsa
    if (merged.paid_off < -1e-6).any():
        raise ValueError(
            "Found negative Paid off capacities. This indicates that the harmonized PyPSA capacities "
            "exceed the REMIND capacities. Please check the harmonization step."
        )

    return (
        merged.groupby(["tech_group", "year"])
        .paid_off.sum()
        .clip(lower=0)
        .reset_index()
        .rename(columns={"paid_off": "Capacity"})
    )
