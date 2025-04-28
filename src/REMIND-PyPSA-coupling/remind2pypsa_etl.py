"""
Extract data from Remind, transform it for pypsa PyPSA and write it to files
"""

import pandas as pd
import os
import country_converter as coco
from utils import read_remind_csv, read_remind_regions_csv, read_remind_descriptions_csv

UNIT_CONVERSION = {
    "capex": 1e6,  # TUSD/TW(h) to USD/MW(h)
    "VOM": 1e6 / 8760,  # TUSD/TWa to USD/MWh
    "FOM": 100,  # p.u to percent
}


def make_pypsa_like_costs(tech_data_p: os.PathLike, capex_data_path: os.PathLike) -> pd.DataFrame:
    """translate the REMIND costs into pypsa format.
    Args:
        tech_data_p (os.PathLike): path to the REMIND technology data
        capex_data_path (os.PathLike): path to the REMIND CAPEX data
    Returns:
        pd.DataFrame: DataFrame containing cost data.
    """
    tech_data = read_remind_csv(
        tech_data_p, names=["variable", "region", "parameter", "technology", "value"], skiprows=1
    )
    capex = read_remind_csv(
        costs_p, names=["variable", "year", "region", "technology", "value"], skiprows=1
    )

    # transform individual data
    capex = transform_capex(capex)
    capex[(capex.year == 2025)].drop(columns=["region"])

    lifetime = transform_lifetime(tech_data.query("parameter == 'lifetime'"))

    vom = tech_data.query("parameter == 'omv'")
    vom = transform_vom(vom)

    fom = tech_data.query("parameter == 'omf'")
    fom = transform_fom(fom)

    # stitch together in pypsa format
    costs_remind = pd.concat([lifetime.query("region == 'CHA'"),vom.query("region == 'CHA'"), fom.query("region == 'CHA'")], axis = 0).drop(columns="region").reset_index(drop = True)
    costs_remind = expand_years(costs_remind, capex.year.unique())
    column_order = ["technology","year","parameter","value","unit","source"]
    costs_remind = pd.concat([costs_remind[column_ord   er], capex[column_order]], axis = 0).reset_index(drop = True)
    costs_remind.sort_values(by = ["technology","year","parameter"], inplace = True)
    
    return costs_remind


def transform_capex(capex: pd.DataFrame) -> pd.DataFrame:
    """Transform the CAPEX data from REMIND to pypsa.
    Args:
        capex (pd.DataFrame): DataFrame containing REMIND capex data.
    Returns:
        pd.DataFrame: Transformed capex data.
    """
    capex.loc[:, "value"] *= UNIT_CONVERSION["capex"]
    capex["unit"] = "USD/MW"
    store_techs = ["h2stor", "btstor", "phs"]
    capex["source"] = "REMIND " + capex.technology
    for stor in store_techs:
        capex.loc[capex["technology"] == stor, "unit"] = "USD/MWh"
    capex["parameter"] = "investment"
    return capex


def transform_vom(vom: pd.DataFrame) -> pd.DataFrame:
    """Transform the Variable Operational Maintenance data from REMIND to pypsa.
    Args:
        vom (pd.DataFrame): DataFrame containing REMIND VOM data.
    Returns:
        pd.DataFrame: Transformed VOM data.
    """
    vom.loc[:, "value"] *= UNIT_CONVERSION["VOM"]
    vom["unit"] = "USD/MWh"
    vom["source"] = vom.technology + " REMIND"
    vom["parameter"] = "VOM"
    return vom


def transform_fom(fom: pd.DataFrame) -> pd.DataFrame:
    """Transform the Fixed Operational Maintenance data from REMIND to pypsa.
    Args:
        vom (pd.DataFrame): DataFrame containing REMIND FOM data.
    Returns:
        pd.DataFrame: Transformed FOM data.
    """
    fom["unit"] = "percent"
    fom.loc[:, "value"] *= UNIT_CONVERSION["FOM"]
    fom["source"] = fom.technology + " REMIND"
    fom["parameter"] = "FOM"
    return fom


def transform_lifetime(lifetime: pd.DataFrame) -> pd.DataFrame:
    """Transform the lifetime data from REMIND to pypsa.

    Args:
        lifetime (pd.DataFrame): DataFrame containing REMIND lifetime data.
    Returns:
        pd.DataFrame: Transformed lifetime data.
    """
    lifetime["unit"] = "years"
    lifetime["source"] = lifetime.technology + " REMIND"
    return lifetime


def expand_years(df: pd.DataFrame, years: list) -> pd.DataFrame:
    """expand the dataframe by the years

    Args:
        df (pd.DataFrame): time-indep data
        years (list): the years

    Returns:
        pd.DataFrame: time-indep data with explicit years
    """

    return pd.concat([df.assign(year=yr) for yr in years])
