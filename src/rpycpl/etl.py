""" ETL TOOL BOX

- Abstracted transformations (Transformation, register_etl)
- ETL registry (list of named conversions)
- pre-defined conversions (convert_loads, technoeconomic_data)"""

import pandas as pd
import logging
from dataclasses import dataclass, field
from typing import Dict, Any, Optional

from .utils import build_tech_map
from .technoecon_etl import (
    validate_mappings,
    validate_remind_data,
    map_to_pypsa_tech,
    to_list,
    make_pypsa_like_costs,
)

logger = logging.getLogger(__name__)
ETL_REGISTRY = {}

def register_etl(name):
    """decorator factory to register ETL functions"""

    def decorator(func):
        ETL_REGISTRY[name] = func
        return func

    return decorator

# TODO cleanup fields
@dataclass
class Transformation:
    """Data class representing the YAML config for the ETL target"""

    name: str
    method: Optional[str] = None
    frames: Dict[str, Any] = field(default_factory=dict)
    params: Dict[str, Any] = field(default_factory=dict)
    filters: Dict[str, Any] = field(default_factory=dict)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    dependencies: Dict[str, Any] = field(default_factory=dict)


@register_etl("build_tech_map")
def build_tech_groups(frames, map_param="investment") -> pd.DataFrame:
    """ Wrapper for the utils.build_tech_map function"""
    return build_tech_map(frames["tech_mapping"], map_param)


@register_etl("convert_load")
def convert_loads(loads: dict[str, pd.DataFrame], region: str = None) -> pd.DataFrame:
    """conversion for loads

    Args:
        loads (dict): dictionary of dataframes with loads
        region (str, Optional): region to filter the data by
    Returns:
        pd.DataFrame: converted loads (year: load type, value in Mwh)
    """
    TWYR2MWH = 365 * 24 * 1e6
    outp = pd.DataFrame()
    for k, df in loads.items():
        df["load"] = k.split("_")[0]
        if ("region" in df.columns) & (region is not None):
            df = df.query("region == @region").drop(columns=["region"])
        df.value *= TWYR2MWH
        outp = pd.concat([outp, df], axis=0)
    return outp.set_index("year")


@register_etl("convert_capacities")
def convert_remind_capacities(frames: dict[str, pd.DataFrame], cutoff=0, region: str = None) -> pd.DataFrame:
    """conversion for capacities

    Args:
        frames (dict): dictionary of dataframes with capacities
        region (str, Optional): region to filter the data by
        cutoff (int, Optional): min capacity in MW
    Returns:
        pd.DataFrame: converted capacities (year: load type, value in Mwh)
    """
    TW2MW = 1e6
    caps = frames["capacities"]
    caps.loc[:, "value"] *= TW2MW

    if ("region" in caps.columns) & (region is not None):
        caps = caps.query("region == @region").drop(columns=["region"])

    too_small = caps.query("value < @cutoff").index
    caps.loc[too_small, "value"] = 0

    if "tech_groups" in frames:
        tech_map = frames["tech_groups"]
        caps.loc[:, "tech_group"] = caps.technology.map(tech_map.group.to_dict())

    return caps.rename(columns={"value": "capacity"}).set_index("year")


@register_etl("technoeconomic_data")
def technoeconomic_data(
    frames: Dict[str, pd.DataFrame], mappings: pd.DataFrame, pypsa_costs: pd.DataFrame
) -> pd.DataFrame:
    """Mapping adapted from Johannes Hemp, based on csv mapping table
    
    Args:
        frames (Dict[str, pd.DataFrame]): dictionary of remind frames
        mappings (pd.DataFrame): the mapping dataframe
        pypsa_costs (pd.DataFrame): pypsa costs dataframe
    Returns:
        pd.DataFrame: dataframe with the mapped techno-economic data
    """

    # explode multiple references into rows
    mappings.loc[:, "reference"] = mappings["reference"].apply(to_list)

    # check the data & mappings
    validate_mappings(mappings)

    # maybe do something nicer but should be ok if remind export is correct
    years = frames["capex"].year.unique()

    weight_frames = [
        frames[k].assign(weight_type=k) for k in frames if k.startswith("weights")
    ]
    weights = pd.concat(
        [
            df.rename(columns={"carrier": "technology", "value": "weight"})
            for df in weight_frames
        ]
    )

    costs_remind = make_pypsa_like_costs(frames)
    costs_remind = costs_remind.merge(weights, on=["technology", "year"], how="left")

    validate_remind_data(costs_remind, mappings)

    mappings.loc[:, "reference"] = mappings["reference"].apply(to_list)

    # apply the mappings to pypsa tech
    mapped_costs = map_to_pypsa_tech(
        remind_costs_formatted=costs_remind,
        pypsa_costs=pypsa_costs,
        mappings=mappings,
        weights=weights,
        years=years,
    )
    mapped_costs["value"].fillna(0, inplace=True)
    mapped_costs.fillna(" ", inplace=True)

    return mapped_costs

