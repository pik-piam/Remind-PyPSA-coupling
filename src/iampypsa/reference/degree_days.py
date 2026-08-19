"""Read cooling/heating degree-day (CDD/HDD) proxy datasets used to weight downscaling.

Degree-day CSVs carry columns ``[year, country, type, tlim_setpoint, rcp, ssp, value]`` with
``country`` as ISO3 and ``type`` one of ``CDD``/``HDD``. ``read_degree_days`` selects one
``(type, tlim_setpoint, rcp, ssp)`` combination and returns the ``[iso2, year, value]`` frame
used as a downscaling proxy — the same shape as :func:`iampypsa.io.ssp.read_ssp_data`, so callers do
``.set_index(["iso2", "year"])`` and pass it in the ``proxies`` registry.
"""

import logging
from os import PathLike

import country_converter as coco
import pandas as pd

logger = logging.getLogger(__name__)

_VALID_TYPES = {"CDD", "HDD"}


def read_degree_days(
    path: str | PathLike,
    *,
    dd_type: str,
    tlim_setpoint: float,
    rcp: str,
    ssp: str,
) -> pd.DataFrame:
    """Read a degree-day CSV and return the ``[iso2, year, value]`` proxy frame.

    Filters to ``type == dd_type`` (``CDD``/``HDD``), the given ``tlim_setpoint``, ``rcp`` and
    ``ssp``; converts ISO3 → ISO2 (unmapped codes dropped) and sums any duplicates. Raises
    ``ValueError`` for an unknown ``dd_type`` or a selection matching no rows (the message lists the
    available values to ease debugging — e.g. a ``tlim_setpoint`` outside the data's range).
    """
    dd_type = str(dd_type).upper()
    if dd_type not in _VALID_TYPES:
        raise ValueError(f"dd_type must be one of {sorted(_VALID_TYPES)}, got {dd_type!r}.")

    df = pd.read_csv(path)
    df["type"] = df["type"].astype(str).str.upper()
    typed = df[df["type"] == dd_type]
    if typed.empty:
        raise ValueError(
            f"No {dd_type} rows in {path} (types present: {sorted(df['type'].unique())})."
        )

    # rcp/ssp compared as strings (e.g. "4_5", "SSP2"); tlim_setpoint numerically.
    selected = typed[
        (typed["tlim_setpoint"] == tlim_setpoint)
        & (typed["rcp"].astype(str) == str(rcp))
        & (typed["ssp"].astype(str) == str(ssp))
    ]
    if selected.empty:
        raise ValueError(
            f"No {dd_type} rows for tlim_setpoint={tlim_setpoint}, rcp={rcp!r}, ssp={ssp!r} "
            f"in {path}. Available: tlim_setpoint={sorted(typed['tlim_setpoint'].unique())}, "
            f"rcp={sorted(typed['rcp'].astype(str).unique())}, "
            f"ssp={sorted(typed['ssp'].astype(str).unique())}."
        )

    selected = selected.copy()
    # Pin src=ISO3: the country column is uniformly ISO3, so avoid coco's fuzzy source detection.
    selected["iso2"] = coco.CountryConverter().convert(
        selected["country"].tolist(), to="ISO2", src="ISO3", not_found=None
    )
    return (
        selected.dropna(subset=["iso2"])[["iso2", "year", "value"]]
        .groupby(["iso2", "year"], as_index=False)["value"]
        .sum()
        .sort_values(["iso2", "year"])
        .reset_index(drop=True)
    )
