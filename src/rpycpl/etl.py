""" ETL TOOL BOX

- Abstracted transformations (Transformation, register_etl)
- ETL registry (list of named conversions)
- pre-defined conversions (convert_loads, technoeconomic_data)"""

import pandas as pd
import logging
from dataclasses import dataclass, field
from typing import Dict, Any, Optional

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


@dataclass
class Transformation:
    """Data class representing the YAML config for the ETL target"""

    name: str
    method: Optional[str] = None
    frames: Dict[str, Any] = field(default_factory=dict)
    params: Dict[str, Any] = field(default_factory=dict)
    filters: Dict[str, Any] = field(default_factory=dict)
    kwargs: Dict[str, Any] = field(default_factory=dict)


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


@register_etl("technoeconomic_data")
def technoeconomic_data(
    frames: Dict[str, pd.DataFrame], mappings: pd.DataFrame, pypsa_costs: pd.DataFrame
) -> pd.DataFrame:
    """Mapping adapted from Johannes Hemp, based on csv mapping table"""

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
