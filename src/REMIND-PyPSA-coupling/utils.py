""" Utility functions for the REMIND-PyPSA coupling"""

import os
import pandas as pd
import country_converter as coco


def read_remind_regions(mapping_path: os.PathLike, separator=",") -> pd.DataFrame:
    """read the export from remind

    Args:
        mapping_path (os.PathLike): the path to the remind mapping (csv export of regi2iso set via GamsConnect)

    Returns:
        pd.DataFrame: the region mapping
    """
    regions = pd.read_csv(mapping_path)
    regions.drop(columns="element_text", inplace=True)
    regions["iso2"] = coco.convert(regions["iso"], to="ISO2")
    return regions
