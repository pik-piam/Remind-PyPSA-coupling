"""Fetch / read SSP population & GDP proxy datasets used to weight downscaling.

``fetch_ssp_variable`` pulls a country-level IAMC variable directly from the IIASA SSP
REST API (no auth/pyam needed); ``read_ssp_data`` reads a saved snapshot.
Output columns: ``[iso2, year, value]``.
"""

import logging
from collections.abc import Sequence
from os import PathLike
import country_converter as coco
import pandas as pd

logger = logging.getLogger(__name__)

#: Default IIASA SSP REST endpoint. Uses plain REST via httpx instead of ``pyam``/``ixmp4``,
#: since those pull in a heavy dependency tree (including a DB client) just to download two
#: variables — not worth it for an optional extra. Switch if we ever need pyam's query/units
#: features beyond a flat tabulate.
IIASA_URL = "https://ixmp4.ece.iiasa.ac.at/v1/ssp/iamc/datapoints/tabulate"
_PAGE_SIZE = 20_000


def fetch_ssp_variable(
    variable: str,
    model: str,
    scenario: str,
    *,
    url: str = IIASA_URL,
    page_size: int = _PAGE_SIZE,
    timeout: float = 120.0,
) -> pd.DataFrame:
    """Fetch one IAMC variable from the IIASA SSP API as ``[iso2, year, value]``."""
    import httpx  # optional 'ssp' extra — imported lazily so `import iampypsa` works without it

    body = {
        "join_parameters": True,
        "join_runs": True,
        "join_run_id": False,
        "variable": {"name__like": variable},
        "run": {"default_only": True},
        "model": {"name__like": model},
        "scenario": {"name__like": scenario},
    }
    rows: list = []
    columns: list | None = None
    offset = 0
    while True:
        resp = httpx.patch(
            url, params={"limit": page_size, "offset": offset}, json=body, timeout=timeout
        )
        resp.raise_for_status()
        results = resp.json()["results"]
        columns = columns or results["columns"]
        page = results["data"]
        rows.extend(page)
        offset += len(page)
        if len(page) < page_size:
            break

    df = pd.DataFrame(rows, columns=columns)
    df["iso2"] = coco.CountryConverter().pandas_convert(pd.Series(df["region"]), to="ISO2", not_found=None)
    return (
        df.dropna(subset=["iso2"])[["iso2", "step_year", "value"]]
        .rename(columns={"step_year": "year"})
        .groupby(["iso2", "year"], as_index=False)["value"]
        .sum()
        .sort_values(["iso2", "year"])
        .reset_index(drop=True)
    )


def fetch_ssp_data(
    scenario: str,
    population_model: str,
    gdp_model: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch SSP population and GDP|PPP for one scenario; return ``(population, gdp)``."""
    population = fetch_ssp_variable("Population", population_model, scenario)
    gdp = fetch_ssp_variable("GDP|PPP", gdp_model, scenario)
    return population, gdp


def read_ssp_data(
    path: str | PathLike,
    variables: Sequence[str] | None = None,  # accepted for API symmetry; snapshot is single-variable
) -> pd.DataFrame:
    """Read a saved SSP snapshot CSV with columns ``[iso2, year, value]``."""
    return pd.read_csv(path)
