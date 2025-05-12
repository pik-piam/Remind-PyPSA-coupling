""" 
Disaggregation tools 
- Spatial disaggregation
- Temporal disaggregation
"""

import pandas as pd
import numpy as np


class SpatialDisaggregator:

    def use_static_reference(self, data: pd.Series, reference_data: pd.Series):
        """
        Use a reference year to disaggregate the quantity spatially
        Args:
            data (pd.Series): The data to be disaggregated. Dims: (year,).
            reference_data (pd.Series): The reference data for disaggregation.
                E.g the distribution for a reference year. Dims: (space,).
        Returns:
            pd.DataFrame: The disaggregated data. Dims: (space, year).
        """
        if not reference_data.sum() == 1:
            raise ValueError("Reference data is not normalised to 1")

        # outer/cartersian product to get (years, region) matrix
        return pd.DataFrame(
            np.outer(data, reference_data), index=data.index, columns=reference_data.index
        ).T
