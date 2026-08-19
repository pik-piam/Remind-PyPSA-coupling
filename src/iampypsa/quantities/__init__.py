"""What a coupling name means: its source name(s), its units, and how to load it.

``config`` layers the YAML (packaged default + overlay + region overrides), ``load`` turns one
resolved spec into a canonical frame, ``conversion`` applies the declared units, and ``schema``
holds the pure spec/technology-mapping logic. Backend-specific loaders deliberately do **not**
live here — they live with their format in :mod:`iampypsa.formats`.
"""

from iampypsa.quantities.config import (
    load_quantity_specs,
    load_technology_parameters,
    merge_region_overrides,
    read_quantity_config,
)
from iampypsa.quantities.load import (
    load_quantity,
    load_simple,
    rename_technologies,
    report_fallbacks,
)

__all__ = [
    "load_quantity",
    "load_quantity_specs",
    "load_simple",
    "load_technology_parameters",
    "merge_region_overrides",
    "read_quantity_config",
    "rename_technologies",
    "report_fallbacks",
]
