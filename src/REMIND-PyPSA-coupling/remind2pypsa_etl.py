"""
Extract data from Remind, transform it for pypsa PyPSA and write it to files
"""

# TODO add Cb/Cv

# TODO centralise remind column name mapping and use a set -> name

# TODO add remind model version to the cost provenance

import pandas as pd
import os
import country_converter as coco
from collections.abc import Iterable

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
    "weights_gen": "p32_weightGen",
}


# TODO add actual functions
MAPPING_FUNCTIONS = [
    "set_value",
    "use_remind",
    "use_remind_with_learning_from",
    "use_pypsa",
    "weigh_remind_by_gen",
    "weigh_remind_by_capacity",
]


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

    # TODO Can do more efficient operations with join

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
    fuels = fuels.assign(technology=fuels.carrier)
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


def map_to_pypsa_tech(
    remind_costs_formatted: pd.DataFrame,
    pypsa_costs: pd.DataFrame,
    mappings: pd.DataFrame,
    years: list | Iterable = None,
) -> pd.DataFrame:
    """Map the REMIND technology names to pypsa technoloies using the conversions specified in the
    map config

    Args:
        cost_frames (pd.DataFrame): DataFrame containing REMIND cost data.
        pypsa_costs (pd.DataFrame): DataFrame containing pypsa cost data.
        mappings (pd.DataFrame): DataFrame containing the mapping funcs and names from REMIND to pypsa technologies.
        years (Iterable, optional): years to be used. Defaults to None (use remidn dat)
    Returns:
        pd.DataFrame: DataFrame with mapped technology names.
    """
    if not years:
        years = remind_costs_formatted.year.unique()

    # direct mapping of remind
    use_remind = (
        mappings.query("mapper == 'use_remind'")
        .drop("unit", axis=1)
        .merge(
            remind_costs_formatted,
            left_on=["reference", "parameter"],
            right_on=["technology", "parameter"],
            how="left",
        )
    )

    direct_input = mappings.query("mapper == 'set_value'").rename(columns={"reference": "value"})
    direct_input.assign(source="direct_input from coupling mapping")

    # pypsa values
    from_pypsa = _use_pypsa(mappings, pypsa_costs, years)

    # techs with proxy learnign
    proxy_learning = _learn_investment_from_proxy(
        mappings, pypsa_costs, remind_costs_formatted, ref_year=years.min()
    )

    # TODO add weigh_remind_by

    # TODO concat

    # TODO check lengths are as expected


# TODO ? move to a class
def _learn_investment_from_proxy(
    mappings: pd.DataFrame,
    pypsa_costs: pd.DataFrame,
    remind_costs_formatted: pd.DataFrame,
    ref_year: int,
):
    """For techs missing in REMIND, take a pypsa tech and apply learning from a proxy REMIND tech

    Args:
        mappings (pd.DataFrame): DataFrame containing the tech mappings from REMIND to pypsa.
        pypsa_costs (pd.DataFrame): DataFrame containing pypsa cost data.
        remind_costs_formatted (pd.DataFrame): DataFrame containing REMIND cost data (pypsa-like).
        ref_year (int): reference year for scaling
    Returns:
        pd.DataFrame: DataFrame with scaled investment costs.
    """

    ref_tech_names = mappings.query("mapper == 'use_remind_with_learning_from'")[
        ["PyPSA_tech", "reference"]
    ].set_index("reference")
    # if mapping is empty for use_reminfd_with_learning_from, return empty
    if not ref_tech_names.shape[0]:
        return pd.DataFrame()

    ref_tech_names.reset_index(inplace=True)
    ref_tech_names.rename(columns={"reference": "technology"}, inplace=True)

    base_yr_investmnt = pypsa_costs.query(
        "technology in @ref_tech_names.PyPSA_tech.unique() & year == @ref_year & parameter == 'investment'"
    ).set_index("technology")
    base_yr_investmnt = base_yr_investmnt.value.to_dict()

    # TODO check all references are available
    scaling = remind_costs_formatted.query(
        "technology in @ref_tech_names.technology & parameter == 'investment'"
    )

    scaling.loc[:, "value"] = (
        scaling.set_index(["technology"])
        .groupby(level=[0])
        .apply(lambda x: x.value / x[x.year == x.year.min()].value)
        .values
    )

    # merge
    proxy_invest = scaling.merge(
        ref_tech_names,
        left_on="technology",
        right_on="technology",
        how="left",
        right_index=False,
        left_index=False,
    )
    proxy_invest.technology = proxy_invest.PyPSA_tech
    proxy_invest.drop(columns=["PyPSA_tech"], inplace=True)

    # multiply the scaling factor with the base year investment
    proxy_invest.loc[:, "value"] = (
        proxy_invest.technology.map(base_yr_investmnt) * proxy_invest.value
    )

    return proxy_invest


def _use_pypsa(
    mappings: pd.DataFrame,
    pypsa_costs: pd.DataFrame,
    years: Iterable,
    extrapolation="constant",
) -> pd.DataFrame:
    """Use the pypsa costs for requested technologies (e.g. are not in REMIND)

    Args:
        mappings (pd.DataFrame): DataFrame containing the tech REMIND to pypsa mapping
        pypsa_costs (pd.DataFrame): DataFrame containing pypsa cost data.
        years (Iterable): data years to be used
        extrpolation (str, Optional): how to handle missing years.
            Defaults to "constant_extrapolation" (last data yr used for missing)

    Returns:
        pd.DataFrame: DataFrame with mapped technology data.
    """

    from_pypsa = mappings.query("mapper == 'use_pypsa'").merge(
        pypsa_costs,
        left_on=["PyPSA_tech", "parameter"],
        right_on=["technology", "parameter"],
        how="left",
    )

    from_pypsa.rename(columns={"unit_x": "expected_unit", "unit_y": "unit"}, inplace=True)
    from_pypsa.reference = from_pypsa.source

    # === Add missing years to the pypsa data using the last pypsa year ===
    missing_yrs = set(years).difference(from_pypsa.year.unique())

    missing_yrs = [float(yr) for yr in missing_yrs]
    if (list(missing_yrs) < from_pypsa.year.max()).any():
        raise ValueError("The PyPSA data is missing years before its last year")

    if not missing_yrs:
        pass
    elif extrapolation == "constant":
        final_yr_data = from_pypsa.query("year==@from_pypsa.year.max()")
        constant_extrapol = pd.concat([final_yr_data.assign(year=yr) for yr in missing_yrs])
        pd.concat([from_pypsa, constant_extrapol]).reset_index(drop=True)
    else:
        raise ValueError(f"Unknown extrapolation method: {extrapolation}")

    # Validate pypsa completeness
    if from_pypsa[from_pypsa.year.isna()].parameter.any():
        raise ValueError(
            f"Missing data in pypsa data for {from_pypsa[from_pypsa.year.isna()].PyPSA_tech} "
            "Check the mappings and the pypsa data"
        )
    return from_pypsa


def _weigh_remind_by(
    remind_costs_formatted: pd.DataFrame, weights: pd.DataFrame, mappings: pd.DataFrame
) -> pd.DataFrame:
    """Weigh the REMIND costs by the weights

    Args:
        remind_costs_formatted (pd.DataFrame): DataFrame containing REMIND cost data.
        weights (pd.DataFrame): DataFrame containing the weights.
        mappings (pd.DataFrame): DataFrame containing the tech mappings from REMIND to pypsa.

    Returns:
        pd.DataFrame: DataFrame with weighed technology names.
    """

    # entries that need to be weighted accross remind techs
    to_weigh = mappings.query("mapper.str.startswith('weigh_remind_by_')")
    to_weigh = to_weigh.assign(weigh_by=to_weigh["mapper"].str.split("weigh_remind_by_").str[1])

    # explode list of weight techs (rows dim)
    weightings = to_weigh.explode("reference").reset_index()
    weightings.rename(columns={"index": "weightgroup", "unit": "map_unit"}, inplace=True)

    # apply the weights (use the original row id as grouping)
    to_weigh.loc[:, "value"] = weightings.groupby("id_weight").apply(
        lambda x: (x.value * x.weight).sum() / x.weight.sum()
    )

    # validate that units matched (unique) # !! should check nans too
    units_ok = weightings.groupby("id_weight").unit.apply(pd.Series.nunique) == 1
    if not units_ok.all():
        named_unit_check = pd.concat([to_weigh, units_ok], axis=1)[["PyPSA_tech", "unit"]]
        raise ValueError(
            "Units not do not match for weights:", named_unit_check[~named_unit_check["unit"]]
        )

    return to_weigh


# TODO make mappings a dataclass not a pandas
def validate_mappings(mappings: pd.DataFrame):
    """validate the mapping of the technologies to pypsa technologies
    Args:
        mappings (pd.DataFrame): DataFrame containing the mapping funcs and names from REMIND to pypsa technologies.
    Raises:
        ValueError: if mappers not allowed
        ValueError: if columns not expected
        ValueError: if proxy learning (use_remind_with_learning_from) is used for something other than invest

    """

    # validate columns
    EXPECTED_COLUMNS = ["PyPSA_tech", "parameter", "mapper", "reference", "unit", "comment"]
    if not sorted(mappings.columns) == sorted(EXPECTED_COLUMNS):
        raise ValueError(
            f"Invalid mapping. Allowed columns are {["PyPSA_tech","parameter","mapper","reference","unit","comment"]}"
        )

    # validate mappers allowed
    forbidden_mappers = set(mappings.mapper.unique()).difference(MAPPING_FUNCTIONS.keys())
    if forbidden_mappers:
        raise ValueError(f"Forbidden mappers found in mappings: {forbidden_mappers}")

    # validate proxy learning
    proxy_learning = mappings.query("mapper == 'use_remind_with_learning_from'")
    proxy_params = set(proxy_learning.parameter)
    if proxy_params.difference({"investment"}):
        raise ValueError(f"Proxy learning is only allowed for investment but Found: {proxy_params}")

    # validate numeric
    set_vals = mappings.query("mapper == 'set_value'")["reference"]
    try:
        set_vals.astype(float)
    except ValueError as e:
        raise ValueError(f"set_value reference values must be numeric but: {e}")

    # check uniqueness
    counts = mappings.groupby(["PyPSA_tech", "parameter"]).count()
    repeats = counts[counts.values > 1]
    if len():
        raise ValueError(f"Mappings are not unique: n repeats:\n {repeats} ")
        # should validate that remind references are actually in the remind export


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
    # TODO remove region filters everywhere
    # frames = {k: df.query("region == @region") if "region" in df.columns for k, df in frames.items()}

    # make the stitched weight frames
    weight_frames = [frames[k].assign(weightby=k) for k in frames if k.startswith("weights")]
    weights = pd.concat(
        [
            df.rename(columns={"carrier": "technology", "value": "weight"})
            .query("region==@region")
            .drop(columns="region")
            for df in weight_frames
        ]
    )
    # add weights by techs
    remind_formatted = remind_formatted.merge(weights, on=["technology", "year"], how="left")

    # make a pypsa like cost table, with remind values
    costs_remind = make_pypsa_like_costs(frames, region)

    # load the mapping
    mappings = pd.read_csv(os.path.abspath("../../data") + "/techmapping_remind2py.csv")
    validate_mappings(mappings)

    def _list(x: str):
        """in case of csv input"""
        if isinstance(x, str) and x.startswith("[") and x.endswith("]"):
            return x.replace("[", "").replace("]", "").split(", ")
        return x

    mappings["reference"] = mappings["reference"].apply(_list)

    # load pypsa costs
    pypsa_costs_dir = os.path.abspath("../../../PyPSA-China-PIK/resources/data/costs")
    pypsa_cost_files = [os.path.join(pypsa_costs_dir, f) for f in os.listdir(p)]
    pypsa_costs = pd.read_csv(pypsa_cost_files.pop())
    for f in pypsa_cost_files:
        pypsa_costs = pd.concat([pypsa_costs, pd.read_csv(f)])

    # apply the mappings to pypsa tech
