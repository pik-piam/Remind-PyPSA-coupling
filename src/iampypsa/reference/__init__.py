"""External reference datasets used as downscaling proxies — not IAM output."""

from iampypsa.reference.degree_days import read_degree_days
from iampypsa.reference.ssp import fetch_ssp_data, fetch_ssp_variable, read_ssp_data

__all__ = ["read_degree_days", "fetch_ssp_data", "fetch_ssp_variable", "read_ssp_data"]
