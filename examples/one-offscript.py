
import os
import logging
import pandas as pd

from rpycpl.utils import read_remind_csv

logger = logging.getLogger(__name__)

def make_example_data():
    """mock remind export for testing"""
    ac_load = pd.DataFrame({
        'year': [2030, 2035, 2040, 2045, 2050],
        'region': ['CHA'] * 5,
        'value': [1.396324, 1.450305, 1.488186, 1.602782, 1.650218],
    })
    h2_load = pd.DataFrame({
        'year': [2030, 2035, 2040, 2045, 2050],
        'region': ['CHA'] * 5,
        'value': [0 ,0, 0, 0,0],
    })

    ac_load.to_csv("p32_load.csv", index=False)
    h2_load.to_csv("p32_h2elload.csv", index=False)


if __name__ == "__main__":
    region = "CHA"
    ref_year = 2025

    logger.info(f"Processing REMIND AC load  for region {region}.")
    TWYR2MWH = 365 * 24 * 1e6

    base_p = os.path.abspath(".")
    logger.info("Loading and transforming load data")
    electricity_demand = (
        read_remind_csv(os.path.join(base_p, "p32_load.csv"))
        .query("region == @region")
        .drop(columns=["region"])
        .set_index("year")
        * TWYR2MWH
    )
