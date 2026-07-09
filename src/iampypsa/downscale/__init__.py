"""Spatial downscaling: distribute coarse values to finer units via proxy shares."""

from __future__ import annotations

from iampypsa.downscale.base import Downscaler, ProportionalDownscaler
from iampypsa.downscale.demand import disaggregate_demand_to_country
from iampypsa.downscale.proxy import (
    build_demand_proxy_from_dd,
    build_proxy_shares,
    build_ssp_shares,
    normalise,
)

__all__ = [
    "Downscaler",
    "ProportionalDownscaler",
    "disaggregate_demand_to_country",
    "build_demand_proxy_from_dd",
    "build_proxy_shares",
    "build_ssp_shares",
    "normalise",
]
