"""Read technology / region mappings (CSV-driven, model-agnostic)."""

from __future__ import annotations

from os import PathLike

import pandas as pd


def read_region_map(
    fn: str | PathLike,
    source: str = "REMIND-EU",
    target: str = "PyPSA-EUR",
    flatten: bool = False,
) -> dict:
    """Read a REMIND region↔country map as ``{source: [target, ...]}``.

    Ported from PyPSA-Eur ``_helpers.get_region_mapping``. Reads the ``;``-separated REMIND-EU
    mapping (columns ``RegionCode``/``CountryCode``), converts ISO3 country codes to ISO2, and
    adds Kosovo (XK → NES). ``source``/``target`` are ``"REMIND-EU"`` or ``"PyPSA-EUR"``.
    """
    import country_converter as coco

    region_mapping = pd.read_csv(fn, sep=";").rename(columns={"RegionCode": "remind-eu"})
    region_mapping["pypsa-eur"] = coco.convert(names=region_mapping["CountryCode"], to="ISO2")
    region_mapping = region_mapping[["pypsa-eur", "remind-eu"]]

    # Kosovo: PyPSA-Eur uses "XK" (not recognised by country_converter); part of NES.
    region_mapping = pd.concat(
        [region_mapping, pd.DataFrame({"pypsa-eur": ["XK"], "remind-eu": ["NES"]})]
    ).reset_index(drop=True)

    grouped = region_mapping.groupby(source.lower())[target.lower()].apply("unique").apply(list)
    if flatten:
        grouped = grouped.apply(lambda x: x[0])
    return grouped.to_dict()
