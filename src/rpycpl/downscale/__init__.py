"""Spatial downscaling: distribute coarse values to finer units via proxy shares."""

from __future__ import annotations

from rpycpl.downscale.base import Downscaler, ProportionalDownscaler
from rpycpl.downscale.demand import disaggregate_demand_to_country
from rpycpl.downscale.proxy import build_demand_proxy, build_proxy_shares, build_ssp_shares, normalise

__all__ = [
    "Downscaler",
    "ProportionalDownscaler",
    "disaggregate_demand_to_country",
    "build_demand_proxy_from_dd",
    "build_proxy_shares",
    "build_ssp_shares",
    "normalise",
]
