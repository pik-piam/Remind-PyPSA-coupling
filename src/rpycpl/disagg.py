"""
Disaggregation tools for:\n
- Spatial disaggregation
- Temporal disaggregation
"""

import pandas as pd
import numpy as np


class SpatialDisaggregator:

    def __init__(self, targets=None):
        self._target_nodes = targets

    def validate_reference_data(self, reference_data: pd.Series):
        """Check reference data has expected format

        Args:
            reference_data (pd.Series): The reference data for disaggregation.
        Raises:
            TypeError: If reference data is not a pandas Series.
            ValueError: If reference data index does not match target nodes.
            ValueError: If reference data is not normalised to 1.
        """

        if not isinstance(reference_data, pd.Series):
            raise TypeError("Reference data must be a pandas Series")
        if self._target_nodes:
            if not reference_data.index.isin(self._target_nodes).all():
                raise ValueError(
                    f"Reference data index {reference_data.index} does not match target nodes {self._target_nodes}"
                )
        if not np.isclose(reference_data.sum(), 1.0, rtol=1e-12):
            raise ValueError("Reference data is not normalised to 1")

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

        self.validate_reference_data(reference_data)
        # outer/cartersian product to get (years, region) matrix
        return pd.DataFrame(
            np.outer(data, reference_data),
            index=data.index,
            columns=reference_data.index,
        ).T
