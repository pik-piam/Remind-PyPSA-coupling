"""Downscaler interface and the generic proportional implementation.

Region→country and country→bus are the same operation: distribute a coarse value over
its finer members proportionally to *proxy* shares (population/GDP/SSP activity, ...).
The shares are built by :mod:`iampypsa.downscale.proxy`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class Downscaler(ABC):
    """Define how coarse values are distributed to finer units."""

    @abstractmethod
    def downscale(self, coarse: pd.DataFrame, proxy_shares: pd.DataFrame) -> pd.DataFrame:
        """Distribute coarse values to finer units using proxy shares."""
        ...


class ProportionalDownscaler(Downscaler):
    """Distribute coarse values to finer units in proportion to proxy shares."""

    def downscale(
        self,
        coarse: pd.DataFrame,
        proxy_shares: pd.DataFrame,
        *,
        coarse_id: str = "region",
        fine_id: str = "fine_id",
        share_col: str = "share",
        value_col: str = "value",
    ) -> pd.DataFrame:
        """Multiply each coarse value by its members' proxy shares.

        ``proxy_shares`` has columns ``[coarse_id, fine_id, share]`` (shares summing to 1
        within each ``coarse_id``); returns the coarse frame re-keyed to ``fine_id`` with
        ``value`` split by share.
        """
        merged = coarse.merge(proxy_shares, on=coarse_id, how="left")
        merged[value_col] = merged[value_col] * merged[share_col]
        return merged.drop(columns=[coarse_id, share_col]).rename(columns={fine_id: coarse_id})
