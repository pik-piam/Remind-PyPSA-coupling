"""Read technology / region mappings (CSV-driven, model-agnostic)."""

from __future__ import annotations

from os import PathLike

import pandas as pd
import country_converter as coco


def read_region_map(
    fn: str | PathLike,
    source: str,
    target: str,
    flatten: bool = False,
) -> dict:
    """Read a region↔country map as ``{source: [target, ...]}``.

    Reads the ``;``-separated mapping CSV (columns ``RegionCode``/``CountryCode``), converts
    ISO3 country codes to ISO2, and adds Kosovo (XK → NES). Pass ``source``/``target`` as
    ``"model_region"`` or ``"country"`` to select the groupby direction.
    """
    region_mapping = pd.read_csv(fn, sep=";").rename(columns={"RegionCode": "model_region"})
    region_mapping["country"] = coco.convert(names=region_mapping["CountryCode"], to="ISO2")
    region_mapping = region_mapping[["country", "model_region"]]

    # Kosovo: PyPSA-Eur uses "XK" (not recognised by country_converter); part of NES.
    region_mapping = pd.concat(
        [region_mapping, pd.DataFrame({"country": ["XK"], "model_region": ["NES"]})]
    ).reset_index(drop=True)

    grouped = region_mapping.groupby(source)[target].apply("unique").apply(list)
    if flatten:
        grouped = grouped.apply(lambda x: x[0])
    return grouped.to_dict()
