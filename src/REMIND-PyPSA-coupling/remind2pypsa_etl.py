"""
Extract data from Remind, transform it for pypsa PyPSA and write it to files
"""

# TODO centralise remind column name mapping and use a set -> name

# TODO add remind model version to the cost provenance

import pandas as pd
import os
import country_converter as coco
from utils import read_remind_csv, read_remind_regions_csv, read_remind_descriptions_csv

MW_C = 12  # g/mol
MW_CO2 = 2 * 16 + MW_C  # g/mol
UNIT_CONVERSION = {
    "capex": 1e6,  # TUSD/TW(h) to USD/MW(h)
    "VOM": 1e6 / 8760,  # TUSD/TWa to USD/MWh
    "FOM": 100,  # p.u to percent
    "co2_intensity": 1e9 * (MW_CO2 / MW_C) / 8760 / 1e6,  # Gt_C/TWa to t_CO2/MWh
}

STOR_TECHS = ["h2stor", "btstor", "phs"]
REMIND_PARAM_MAP = {
    "tech_data": "pm_data",
    "capex": "p32_capCost",
    "eta": "pm_eta_conv",
    # TODO export converged too
    "fuel_costs": "p32_PEPriceAvg",
    "discount_r": "p32_discountRate",
    "co2_intensity": "pm_emifac",
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
    frames: dict[pd.DataFrame],
    region: str,
) -> pd.DataFrame:
    """translate the REMIND costs into pypsa format for a single region.
    Args:
        frames: dictionary with the REMIND data tables to be transformed
        region (str): region to filter the data
    Returns:
        pd.DataFrame: DataFrame containing cost data for a region.
    """

    years = frames["capex"].year.unique()
    capex = transform_capex(frames["capex"])

    # transform the data
    vom = transform_vom(frames["tech_data"].query("parameter == 'omv'"))
    fom = transform_fom(frames["tech_data"].query("parameter == 'omf'"))
    lifetime = transform_lifetime(frames["tech_data"].query("parameter == 'lifetime'"))

    co2_intens = transform_co2_intensity(frames["co2_intensity"], region, years)
    eta = transform_efficiency(frames["eta"], region, years)
    fuel_costs = transform_fuels(frames["fuel_costs"])
    discount_rate = transform_discount_rate(frames["discount_r"])

    del frames

    # stitch together in pypsa format
    cost_frames = {
        "capex": capex,
        "eta": eta,
        "fuel": fuel_costs,
        "co2": co2_intens,
        "lifetime": lifetime,
        "vom": vom,
        "fom": fom,
        "discount_rate": discount_rate,
    }

    # add years to table with time-indep data
    for label, frame in cost_frames.items():
        if not "year" in frame.columns:
            cost_frames[label] = _expand_years(frame, capex.year.unique())
    # add missing techs for tech agnostic data
    for label, frame in cost_frames.items():
        if not "technology" in frame.columns:
            cost_frames[label] = pd.concat(
                [frame.assign(technology=tech) for tech in capex.technology.unique()]
            )
    # add missing regions to global data
    for label, frame in cost_frames.items():
        if not "region" in frame.columns:
            cost_frames[label] = pd.concat([frame.assign(region=region)])
    column_order = ["technology", "year", "parameter", "value", "unit", "source"]

    # merge the dataframes for the region
    costs_remind = pd.concat(
        [frame.query("region == @region")[column_order] for frame in cost_frames.values()], axis=0
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
    co2_intens = co2_intensity.rename(
        columns={
            "carrier": "from_carrier",
            "carrier_1": "to_carrier",
            "carrier_2": "emission_type",
        },
    )
    co2_intens = co2_intens.query(
        "to_carrier == 'seel' & emission_type == 'co2' & region == @region & year in @years"
    )
    co2_intens = co2_intens.assign(
        parameter="CO2 intensity", unit="t_CO2/MWh_th", source=co2_intens.technology + " REMIND"
    )
    co2_intens.loc[:, "value"] *= UNIT_CONVERSION["co2_intensity"]
    return co2_intens


def transform_discount_rate(discount_rate: pd.DataFrame) -> pd.DataFrame:
    discount_rate = discount_rate.assign(parameter="discount rate", unit="p.u.", source="REMIND")
    return discount_rate


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
    fuels.loc[~(fuels["carrier"] == "peur"), "value"] *= 1e6 / 8760
    fuels = fuels.assign(parameter="fuel", unit="USD/MWh_th")
    fuels = fuels.assign(source=fuels.carrier + " REMIND")
    fuels.loc[fuels["carrier"] == "peur", "unit"] = "USD/g_U"

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


def map_to_pypsa_tech(cost_frames: pd.DataFrame, map: pd.DataFrame) -> pd.DataFrame:
    """Map the REMIND technology names to pypsa technoloies using the conversions specified in the
    map config

    Args:
        cost_frames (pd.DataFrame): DataFrame containing REMIND cost data.
        map (pd.DataFrame): DataFrame containing the mapping funcs and names from REMIND to pypsa technologies.

    Returns:
        pd.DataFrame: DataFrame with mapped technology names.
    """


if __name__ == "__main__":

    region = "CHA"  # China w Maccau, Taiwan

    # make paths
    # the remind export uses the name of the symbol as the file name
    base_path = "/home/ivanra/documents/gams_learning/pypsa_export/"
    paths = {
        key: os.path.join(base_path, value + ".csv") for key, value in REMIND_PARAM_MAP.items()
    }

    # load the data
    frames = {k: read_remind_csv(v) for k, v in paths.items()}

    # make a pypsa like cost table
    costs_remind = make_pypsa_like_costs(frames, region)

    # apply the mappings to pypsa tech
