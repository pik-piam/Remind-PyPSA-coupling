""" ETL TOOL BOX"""
from dataclasses import dataclass, field
from typing import Dict, Any, Optional
import pandas as pd

import logging

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

@register_etl("reference_dist")
def reference_dist(remind_load, reference, ref_year=None):
    # ... your ETL logic ...
    return f"reference_dist({remind_load}, {reference}, {ref_year})"


# @register_etl("technoeconomic_data")
# def technoeconomic_data(frames: Dict[str, pd.DataFrame], params: Dict[str, Any]) -> pd.DataFrame:
#     # ... your ETL logic ...
#     return f"technoeconomic_data()"