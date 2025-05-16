"""functions copied over from pypsa-china workflow to facilitate the development
without full coupling"""

import os
import logging
import numpy as np
import pandas as pd

logger = logging.getLogger()


def assign_year_bins(df: pd.DataFrame, year_bins: list) -> pd.DataFrame:
    """
    Assign a year bin to the existing capacities according to the config

    Args:
        df (pd.DataFrame): DataFrame with existing capacities and build years (DateIn)
        year_bins (list): years to bin the existing capacities to
    """

    df_ = df.copy()
    # bin by years (np.digitize)
    df_["grouping_year"] = np.take(year_bins, np.digitize(df.DateIn, year_bins, right=True))
    return df_.fillna(0)


def fix_existing_capacities(
    existing_df: pd.DataFrame, costs: pd.DataFrame, year_bins: list, baseyear: int
) -> pd.DataFrame:
    """add/fill missing dateIn, drop expired assets, drop too new assets

    Args:
        existing_df (pd.DataFrame): the existing capacities
        costs (pd.DataFrame): the technoeconomic data
        year_bins (list): the year groups
        baseyear (int): the base year (run year)

    Returns:
        pd.DataFrame: _description_
    """
    existing_df.DateIn = existing_df.DateIn.astype(int)
    # add/fill missing dateIn
    if "DateOut" not in existing_df.columns:
        existing_df["DateOut"] = np.nan
    # names matching costs split across FuelType and Tech, apply to both. Fillna means no overwrite
    lifetimes = existing_df.Fueltype.map(costs.lifetime).fillna(
        existing_df.Tech.map(costs.lifetime)
    )
    existing_df.loc[:, "DateOut"] = existing_df.DateOut.fillna(lifetimes) + existing_df.DateIn

    # TODO go through the pypsa-EUR fuel drops for the new ppmatching style
    # drop assets which are already phased out / decommissioned
    phased_out = existing_df[existing_df["DateOut"] < baseyear].index
    existing_df.drop(phased_out, inplace=True)

    newer_assets = (existing_df.DateIn > max(year_bins)).sum()
    if newer_assets:
        logger.warning(
            f"There are {newer_assets} assets with build year "
            f"after last power grouping year {max(year_bins)}. "
            "These assets are dropped and not considered."
            "Consider to redefine the grouping years to keep them."
        )
        to_drop = existing_df[existing_df.DateIn > max(year_bins)].index
        existing_df.drop(to_drop, inplace=True)

    existing_df["lifetime"] = existing_df.DateOut - existing_df["grouping_year"]

    existing_df.rename(columns={"cluster_bus": "bus"}, inplace=True)
    return existing_df


def read_existing_capacities(paths_dict: dict[str, os.PathLike]) -> pd.DataFrame:
    """Read existing capacities from csv files and format them
    Args:
        paths_dict (dict[str, os.PathLike]): dictionary with paths to the csv files
    Returns:
        pd.DataFrame: DataFrame with existing capacities
    """
    # TODO fix centralise (make a dict from start?)
    carrier = {
        "coal": "coal power plant",
        "CHP coal": "CHP coal",
        "CHP gas": "CHP gas",
        "OCGT": "OCGT gas",
        "solar": "solar",
        "solar thermal": "solar thermal",
        "onwind": "onwind",
        "offwind": "offwind",
        "coal boiler": "coal boiler",
        "ground heat pump": "heat pump",
        "nuclear": "nuclear",
    }
    df_agg = pd.DataFrame()
    for tech in carrier:
        df = pd.read_csv(paths_dict[tech], index_col=0).fillna(0.0)
        df.columns = df.columns.astype(int)
        df = df.sort_index()

        for year in df.columns:
            for node in df.index:
                name = f"{node}-{tech}-{year}"
                capacity = df.loc[node, year]
                if capacity > 0.0:
                    df_agg.at[name, "Fueltype"] = carrier[tech]
                    df_agg.at[name, "Tech"] = tech
                    df_agg.at[name, "Capacity"] = capacity
                    df_agg.at[name, "DateIn"] = year
                    df_agg.at[name, "cluster_bus"] = node

    return df_agg
