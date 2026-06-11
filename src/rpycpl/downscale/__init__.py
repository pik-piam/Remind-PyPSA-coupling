"""Spatial downscaling: distribute coarse values to finer units via proxy shares."""

from __future__ import annotations

from rpycpl.downscale.base import Downscaler, ProportionalDownscaler
from rpycpl.downscale.demand import disaggregate_demand_to_country
from rpycpl.downscale.proxy import build_ssp_shares, normalise

__all__ = [
    "Downscaler",
    "ProportionalDownscaler",
    "disaggregate_demand_to_country",
    "build_ssp_shares",
    "normalise",
]
