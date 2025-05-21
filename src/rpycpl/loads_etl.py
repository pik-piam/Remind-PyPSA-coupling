"""
Load and transform REMIND AC year load data for pypsa:\n
- convert from TWa t MWh
- disaggregate by sector (not implemented)
- disaggregate spatially (one option implemented)

# TODO
- disaggregate by sector
- add config for disaggregation
- add other loads: H2, Heating, Cooling
"""

import os
import logging

from .utils import read_hu_2013_projections, read_remind_csv
from .disagg import SpatialDisaggregator

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    region = "CHA"
    ref_year = 2025

    logger.info(f"Processing REMIND AC load  for region {region}.")
    TWYR2MWH = 365 * 24 * 1e6

    base_p = os.path.expanduser(
        "~/downloads/output_REMIND/SSP2-Budg1000-PyPSAxprt_2025-05-09/pypsa_export"
    )
    logger.info("Loaing and transforming load data")
    electricity_demand = (
        read_remind_csv(os.path.join(base_p, "p32_load.csv"))
        .query("region == @region")
        .drop(columns=["region"])
        .set_index("year")
        * TWYR2MWH
    )

    logger.info("Loading Hu et al. (2013) projections")
    regional_reference = read_hu_2013_projections(
        os.path.expanduser("~/documents/Remind-PyPSA-coupling/data/xiaowei_pypsa_yearly_load.csv")
    )
    regional_reference = regional_reference[str(ref_year)]

    logger.info("Disaggregating load according to Hu et al. demand projections")
    disagg_load = SpatialDisaggregator().use_static_reference(
        electricity_demand, regional_reference
    )

    # export to csv
    disagg_load.to_csv(
        os.path.join(
            "/home/ivanra/documents/Remind-PyPSA-coupling/output", f"{region}_load_disagg.csv"
        )
    )

    logger.info("Load data extracted, transformed and exported.")
