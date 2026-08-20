"""The GAMS data model: symbols indexed by domain sets, independent of the container.

GAMS is the *data model* (sets, parameters, domain columns); GDX is only one container for it —
the same symbols are also dumped one CSV per symbol. So the set-indexed load lives here rather
than in :mod:`iampypsa.formats.gdx`, and stays reusable by any GAMS-shaped source.
"""

from typing import Any

import pandas as pd

from iampypsa.quantities.conversion import convert_column
from iampypsa.quantities.schema import get_quantity_ref

#: Backends whose data follows this model.
BACKENDS = frozenset({"gdx"})


def load_indexed(loader, spec: dict[str, Any]) -> pd.DataFrame:
    """Load a mixed-unit indexed symbol: one symbol whose ``index`` column selects several
    quantities with different units (e.g. ``pm_data`` indexed by ``char`` → lifetime/FOM/VOM).

    The spec's ``schema`` maps each index value to ``{parameter, unit, to_unit}``. Returns a long
    frame with a ``parameter`` column, ``value`` converted per row via the central units table,
    and a ``unit`` column set to the target unit. Index values not in the schema are dropped.
    """
    raw = loader.read(get_quantity_ref(spec), rename_columns=spec.get("rename"))
    index = spec["index"]
    frames = []
    for key, sub in spec["schema"].items():
        part = raw[raw[index] == key].copy()
        if part.empty:
            continue
        if "to_unit" in sub:
            part = convert_column(part, "value", sub.get("unit", sub["to_unit"]), sub["to_unit"])
        part["parameter"] = sub["parameter"]
        part["unit"] = sub.get("to_unit", sub.get("unit"))
        frames.append(part)
    return pd.concat(frames, ignore_index=True) if frames else raw.iloc[0:0].assign(parameter=[], unit=[])


#: Spec shapes this data model can serve.
SPEC_SHAPES = {"indexed": load_indexed}
