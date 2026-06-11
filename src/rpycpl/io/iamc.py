"""Read REMIND/IAM output from the IAMC ``.mif`` exchange format.

Shell for now. An ``.mif`` is a ``;``-separated table with id columns
``Model;Scenario;Region;Variable;Unit`` followed by one year column each; the full
reader will melt it into a tidy long DataFrame ``[model, scenario, region, variable,
unit, year, value]``. Left unimplemented until an IAMC-format coupling run is needed.
"""

from __future__ import annotations

from collections.abc import Sequence
from os import PathLike

import pandas as pd

ID_COLUMNS = ["model", "scenario", "region", "variable", "unit"]


def read_iamc(
    path: str | PathLike,
    variables: Sequence[str] | None = None,
    sep: str = ";",
) -> pd.DataFrame:
    """Read an IAMC ``.mif`` file into a long DataFrame, optionally filtered by variable."""
    raise NotImplementedError("IAMC .mif reader not implemented yet")


def list_iamc_variables(path: str | PathLike, sep: str = ";") -> list[str]:
    """List the IAMC variable names present in a ``.mif`` file."""
    raise NotImplementedError("IAMC .mif reader not implemented yet")
