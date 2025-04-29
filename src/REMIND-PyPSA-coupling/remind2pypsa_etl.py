"""
Extract data from Remind, transform it for pypsa PyPSA and write it to files
"""

# TODO centralise remind column name mapping and use a set -> name

# TODO add remind model version to the cost provenance

import pandas as pd
import os
import country_converter as coco
from utils import read_remind_csv, read_remind_regions_csv, read_remind_descriptions_csv

STOR_TECHS = ["h2stor", "btstor", "phs"]
MW_C = 12  # g/mol
MW_CO2 = 2 * 16 + MW_C  # g/mol
UNIT_CONVERSION = {
    "capex": 1e6,  # TUSD/TW(h) to USD/MW(h)
    "VOM": 1e6 / 8760,  # TUSD/TWa to USD/MWh
    "FOM": 100,  # p.u to percent
    # TODO double check
    "co2_intensity": 1e9 * (MW_CO2 / MW_C) / 8760 / 1e6,  # Gt_C/TWa to t_CO2/MWh
}


def _key_sort(col):
    if col.name == "year":
        return col.astype(int)
    elif col.name == "technology":
        return col.str.lower()
    else:
        return col


def _expand_years(df: pd.DataFrame, years: list) -> pd.DataFrame:
    """expand the dataframe by the years

    Args:
        df (pd.DataFrame): time-indep data
        years (list): the years

    Returns:
        pd.DataFrame: time-indep data with explicit years
    """

    return pd.concat([df.assign(year=yr) for yr in years])


# TODO: soft-coe remind names
def make_pypsa_like_costs(
    paths: dict,
    region: str,
) -> pd.DataFrame:
    """translate the REMIND costs into pypsa format.
    Args:
        paths (dict): dictionary with the paths to the data
        region (str): region to filter the data
    Returns:
        pd.DataFrame: DataFrame containing cost data for a region.
    """
    # TODO split into sep function
    # TODO remove extra years

    # read the data
    tech_data = read_remind_csv(
        paths["pm_data"],
        names=["variable", "region", "parameter", "technology", "value"],
        skiprows=1,
    )
    pypsa_names = ["variable", "year", "region", "technology", "value"]
    capex = read_remind_csv(paths["capex"], names=pypsa_names, skiprows=1)
    eta = read_remind_csv(paths["eta"], names=pypsa_names, skiprows=1)
    fuel_costs = read_remind_csv(paths["fuels"], names=pypsa_names, skiprows=1)
    dscnt_r = read_remind_csv(
        paths["discount_r"], names=["index", "variable", "year", "value"], skiprows=1
    ).drop(columns="index")

    co2_intens = read_remind_csv(
        paths["co2_intensity"],
        names=[
            "variable",
            "year",
            "region",
            "carrier1",
            "carrier2",
            "technology",
            "emission_type",
            "value",
        ],
        skiprows=1,
    )

    # transform individual data
    capex = transform_capex(capex)
    capex[(capex.year == 2025)].drop(columns=["region"])
    years = capex.year.unique()

    vom = transform_vom(tech_data.query("parameter == 'omv'"))
    fom = transform_fom(tech_data.query("parameter == 'omf'"))
    lifetime = transform_lifetime(tech_data.query("parameter == 'lifetime'"))

    transform_co2_intensity(co2_intens, region, years)
    eta = transform_efficiency(eta, years)
    fuel_costs = transform_fuels(fuel_costs)

    discount_rate = transform_discount_rate(dscnt_r)

    # stitch together in pypsa format
    frames = {
        "capex": capex,
        "eta": eta,
        "fuel": fuel_costs,
        "co2": co2_intens,
        "lifetime": lifetime,
        "vom": vom,
        "fom": fom,
        "discount_rate": discount_rate,
    }
    # add missing years
    for label, frame in frames.items():
        if not "year" in frame.columns:
            frames[label] = _expand_years(frame, capex.year.unique())
    # add missing techs
    for label, frame in frames.items():
        if not "technology" in frame.columns:
            frames[label] = pd.concat(
                [frame.assign(technology=tech) for tech in capex.technology.unique()]
            )
    # add missing regions
    for label, frame in frames.items():
        if not "region" in frame.columns:
            frames[label] = pd.concat([frame.assign(region=region)])
    column_order = ["technology", "year", "parameter", "value", "unit", "source"]

    costs_remind = pd.concat(
        [frame.query("region == @region")[column_order] for frame in frames.values()], axis=0
    ).reset_index(drop=True)
    costs_remind.sort_values(by=["technology", "year", "parameter"], key=_key_sort, inplace=True)

    return costs_remind


def transform_capex(capex: pd.DataFrame) -> pd.DataFrame:
    """Transform the CAPEX data from REMIND to pypsa.
    Args:
        capex (pd.DataFrame): DataFrame containing REMIND capex data.
    Returns:
        pd.DataFrame: Transformed capex data.
    """
    capex.loc[:, "value"] *= UNIT_CONVERSION["capex"]
    capex = capex.assign(source="REMIND " + capex.technology, parameter="investment", unit="USD/MW")
    store_techs = STOR_TECHS
    for stor in store_techs:
        capex.loc[capex["technology"] == stor, "unit"] = "USD/MWh"
    return capex


def transform_co2_intensity(
    co2_intensity: pd.DataFrame, region: str, years: list | pd.Index
) -> pd.DataFrame:
    """Transform the CO2 intensity data from REMIND to pypsa.

    Args:
        co2_intensity (pd.DataFrame): DataFrame containing REMIND CO2 intensity data.
        region (str): Region to filter the data
        years (list | pd.Index): relevant years data.

    Returns:
        pd.DataFrame: Transformed CO2 intensity data.
    """
    # TODO Co2 equivalent
    co2_intens = co2_intensity.query(
        "carrier2 == 'seel' & emission_type == 'co2' & region == @region & year in @years"
    )
    co2_intens = co2_intens.assign(
        parameter="CO2 intensity", unit="t_CO2/MWh_th", source=co2_intens.technology + " REMIND"
    )
    co2_intens.loc[:, "value"] *= UNIT_CONVERSION["co2_intensity"]
    return co2_intens


def transform_discount_rate(discount_rate: pd.DataFrame) -> pd.DataFrame:
    discount_rate = discount_rate.assign(parameter="discount rate", unit="p.u.", source="REMIND")
    return discount_rate.drop(columns=["index"])


def transform_efficiency(
    eff_data: pd.DataFrame, region: str, years: list | pd.Index
) -> pd.DataFrame:
    """Transform the efficiency data from REMIND to pypsa.

    Args:
        eff_data (pd.DataFrame): DataFrame containing REMIND efficiency data.
        region (str): Region to filter the data.
        years (list | pd.Index): relevant years.
    Returns:
        pd.DataFrame: Transformed efficiency data.
    """
    eta = eff_data.query("region == @region & year in @years")
    eta = eta.assign(source=eta.technology + " REMIND", unit="p.u.", parameter="efficiency")

    # Special treatment for nuclear: Efficiencies are in TWa/Mt=8760 TWh/Tg_U -> convert to MWh/g_U to match with fuel costs in USD/g_U
    eta.loc[eta["technology"].isin(["fnrs", "tnrs"]), "value"] *= 8760 / 1e6
    eta.loc[eta["technology"].isin(["fnrs", "tnrs"]), "unit"] = "MWh/g_U"
    # Special treatment for battery: Efficiencies in costs.csv should be roundtrip
    eta.loc[eta["technology"] == "btin", "value"] **= 2

    return eta


def transform_fom(fom: pd.DataFrame) -> pd.DataFrame:
    """Transform the Fixed Operational Maintenance data from REMIND to pypsa.
    Args:
        vom (pd.DataFrame): DataFrame containing REMIND FOM data.
    Returns:
        pd.DataFrame: Transformed FOM data.
    """
    fom.loc[:, "value"] *= UNIT_CONVERSION["FOM"]
    fom = fom.assign(source=fom.technology + " REMIND")
    fom = fom.assign(unit="percent", parameter="FOM")

    return fom


def transform_fuels(fuels: pd.DataFrame) -> pd.DataFrame:

    # Unit conversion from TUSD/TWa to USD/MWh
    # Special treatment for nuclear fuel uranium (peur): Fuel costs are originally in TUSD/Mt = USD/g_U (TUSD/Tg) -> adjust unit
    fuels.loc[~(fuels["technology"] == "peur"), "value"] *= 1e6 / 8760
    fuels = fuels.assign(parameter="fuel", unit="USD/MWh_th")
    fuels = fuels.assign(source=fuels.technology + " REMIND")
    fuels.loc[fuels["technology"] == "peur", "unit"] = "USD/g_U"

    return fuels


def transform_lifetime(lifetime: pd.DataFrame) -> pd.DataFrame:
    """Transform the lifetime data from REMIND to pypsa.

    Args:
        lifetime (pd.DataFrame): DataFrame containing REMIND lifetime data.
    Returns:
        pd.DataFrame: Transformed lifetime data.
    """
    lifetime = lifetime.assign(unit="years", source=lifetime.technology + " REMIND", inplace=True)
    return lifetime


def transform_vom(vom: pd.DataFrame) -> pd.DataFrame:
    """Transform the Variable Operational Maintenance data from REMIND to pypsa.
    Args:
        vom (pd.DataFrame): DataFrame containing REMIND VOM data.
    Returns:
        pd.DataFrame: Transformed VOM data.
    """
    vom.loc[:, "value"] *= UNIT_CONVERSION["VOM"]
    vom = vom.assign(unit="USD/MWh", source=vom.technology + " REMIND", parameter="VOM")
    return vom
