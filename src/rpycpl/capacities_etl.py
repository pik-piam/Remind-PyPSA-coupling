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

## Reference Guide

"""

import os
import pandas as pd
import logging


from .utils import read_remind_csv, build_tech_map

logger = logging.getLogger()


def load_remind_capacities(
    f_path: os.PathLike, region: str, cutoff=500
) -> pd.DataFrame:
    """Load capacities from a CSV file.

    Args:
        f_path (os.PathLike): Path to the CSV file.
        region (str): Remind region to filter the data by.
        cutoff (Optional, int): min capacity in MW

    Returns:
        pd.DataFrame: DataFrame containing the capacities data.
    """
    caps = read_remind_csv(f_path).query("region==@region").drop(columns=["region"])

    TW2MW = 1e6
    caps.loc[:, "value"] *= TW2MW

    small = caps.query("value < @cutoff").index
    caps.loc[small, "value"] = 0
    return caps.rename(columns={"value": "capacity"})


def scale_down_pypsa_caps(
    merged_caps: pd.DataFrame, pypsa_caps: pd.DataFrame, tech_groupings: pd.DataFrame
) -> pd.DataFrame:
    """
    Scale down the pypsa capacities to match the remind capacities by tech group.
    Does not scale up the pypsa capacities.

    Scaling is done by groups of techs, which allows n:1 mapping of remind to pypsa techs.

    Args:
        merged_caps (pd.DataFrame): DataFrame with the merged remind and pypsa capacities by tech group.
        pypsa_caps (pd.DataFrame): DataFrame with the pypsa capacities.
        tech_groupings (pd.DataFrame): DataFrame with the  pypsa tech group names.
    """
    merged_caps["fraction"] = merged_caps.capacity_remind / merged_caps.capacity_pypsa

    scalings = merged_caps.copy()
    # do not touch cases where remind capacity is larger than pypsa capacity
    scalings["fraction"] = scalings["fraction"].clip(upper=1)
    scalings.dropna(subset=["fraction"], inplace=True)

    pypsa_caps["tech_group"] = pypsa_caps.Tech.map(tech_groupings.group.to_dict())
    pypsa_caps = pypsa_caps.merge(
        scalings[["tech_group", "fraction"]],
        how="left",
        on="tech_group",
        suffixes=("", "_scaling"),
    )
    pypsa_caps.Capacity = pypsa_caps.Capacity * pypsa_caps.fraction
    return pypsa_caps


def calc_paidoff_capacity(merged: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the aditional paid off capacity available to pypsa from REMIND investment decisions.
    This capacity is not geographically allocated within the remind region network.

    Args:
        merged (pd.DataFrame): DataFrame with the merged remind and pypsa capacities by tech group.
    Returns:
        pd.DataFrame: DataFrame with the available paid off capacity by tech group.
    """
    merged.fillna(0, inplace=True)
    merged["paid_off"] = merged.capacity_remind - merged.capacity_pypsa
    merged.paid_off = merged.paid_off.fillna(0).clip(lower=0)
    return merged.groupby("tech_group").paid_off.sum()


if __name__ == "__main__":
    par_name = "p32_cap"
    baseyear = 2045
    region = "CHA"

    # Define the path to the CSV file
    caps_rempind_p = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "data",
        f"{par_name}.csv",
    )
    pypsa_caps_p = ""
    mapping_p = os.path.abspath("../../data") + "/techmapping_remind2py.csv"
    outp_existing_pypsa = os.path.abspath("../../data") + "/existing_capacities.csv"
    outp_paid_off = os.path.abspath("../../data") + "/paid_off_capacities.csv"

    # Read the remind data, filter for relevant region and year and group by tech
    remind_caps = load_remind_capacities(caps_rempind_p, region, cutoff=500)
    remind_caps = remind_caps[remind_caps.value > 0].query("year == @baseyear")
    remind_caps = remind_caps.groupby("technology").capacity.sum().reset_index()
    remind_caps.head(2)

    # read the base year capacities according to pypsa
    # NB: these should be fixed already (fitlered for naturally retired etc)
    pypsa_caps = pd.read_csv(pypsa_caps_p)
    # agg over the region by tech in order to compare to remind region-level data
    pypsa_agg = pypsa_caps.groupby("Tech").Capacity.sum().reset_index()

    # map remind tech names to pypsa tech names
    # use groups in case it's not a 1:1 mapping.
    mappings = pd.read_csv()
    tech_map = build_tech_map(mappings, map_param="investment")
    tech_groups = tech_map.drop_duplicates(ignore_index=True).set_index("PyPSA_tech")

    # apply the mapping to the remind capacities
    remind_caps.loc[:, "tech_group"] = remind_caps.technology.map(
        tech_map.group.to_dict()
    )
    missing = remind_caps[remind_caps.tech_group.isna()]
    if not missing.empty:
        logger.warning(
            f"The following remind techs could not be mapped onto pypsa and were skipped \n{missing}"
        )
    remind_caps.dropna(inplace=True)

    # apply the mapping to the pypsa capacities
    pypsa_agg.loc[:, "tech_group"] = pypsa_agg.PyPSA_tech.map(
        tech_groups["group"].to_dict()
    )
    pypsa_agg.dropna()

    # merge the remind and pypsa capacities
    merged = pd.merge(
        remind_caps.groupby("tech_group").capacity.sum().reset_index(),
        pypsa_agg.groupby("tech_group").capacity.sum().reset_index(),
        how="left",
        on="tech_group",
        suffixes=("_remind", "_pypsa"),
    )

    # correct the pypsa capacities so they do not exceed the remind capacities
    pypsa_existing_base_year = scale_down_pypsa_caps(merged, pypsa_caps, tech_groups)

    # calculate the capacity pypsa can install at no cost
    avail_cap = calc_paidoff_capacity(merged)

    # write the data to csv
    pypsa_existing_base_year.to_csv(outp_existing_pypsa, index=False)
    avail_cap.to_csv(outp_paid_off, index=False)
