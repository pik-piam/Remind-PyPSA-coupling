"""Central REMIND symbol configuration: load logical→GDX symbol maps and load frames.

Symbol definitions evolve, so they live in YAML (not code) and are *layered*:

1. the package default ships at ``rpycpl/data/remind_symbols.yaml`` (see
   ``default_symbol_config_path()``);
2. a model/run may overlay its own YAML — passed as ``path=`` or via the ``RPYCPL_SYMBOLS``
   environment variable — which is **deep-merged on top** of the default, so the overlay only
   needs to list what differs (a new symbol, a renamed candidate, a region override).

``load_symbol_specs`` is split into debuggable steps: ``read_symbol_config`` (I/O + overlay →
raw ``{default, overrides}`` dict) and ``merge_region_overrides`` (pure per-logical-name merge
→ the flat map). Inspect either on its own when debugging.
"""

from __future__ import annotations

import importlib.resources
import os
from os import PathLike
from typing import Any

import pandas as pd
import yaml

from rpycpl.io.loader import RemindLoader
from rpycpl.units import unit_factor

#: Environment variable holding a path to a symbol-config overlay (deep-merged onto the default).
SYMBOL_CONFIG_ENV = "RPYCPL_SYMBOLS"


def default_symbol_config_path():
    """Return the path to the packaged default symbol config (easy to open/copy/inspect)."""
    return importlib.resources.files("rpycpl.data").joinpath("remind_symbols.yaml")


def _merge_config(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Deep-merge an overlay config onto a base at the (default / overrides[region]) level."""
    out = {
        "default": dict(base.get("default", {})),
        "overrides": {r: dict(v) for r, v in base.get("overrides", {}).items()},
    }
    out["default"].update(overlay.get("default", {}))
    for region, specs in overlay.get("overrides", {}).items():
        out["overrides"].setdefault(region, {}).update(specs)
    return out


def read_symbol_config(path: str | PathLike | None = None) -> dict[str, Any]:
    """Read the raw symbol config (``{default, overrides}``), overlaying a user file if any.

    Always starts from the packaged default. An overlay file (``path``, else the
    ``RPYCPL_SYMBOLS`` env var) is deep-merged on top. Inspect the return value to debug what
    was actually loaded before any region merge.
    """
    base = yaml.safe_load(default_symbol_config_path().read_text())
    overlay_path = path if path is not None else os.environ.get(SYMBOL_CONFIG_ENV)
    if overlay_path:
        base = _merge_config(base, yaml.safe_load(open(overlay_path).read()))
    return base


def merge_region_overrides(config: dict[str, Any], region: str | None) -> dict[str, Any]:
    """Merge ``default`` with ``overrides[region]`` per logical name (region entry wins).

    Pure dict logic (no I/O): ``region=None`` returns ``default`` unchanged; an unknown region
    (absent from ``overrides``) also returns ``default``. Call directly to debug the merge.
    """
    merged = dict(config["default"])
    if region is not None:
        merged.update(config.get("overrides", {}).get(region, {}))
    return merged


def load_symbol_specs(region: str | None = None, path: str | PathLike | None = None) -> dict[str, Any]:
    """Return the resolved symbol map: packaged default (+ optional overlay) merged for ``region``.

    Thin orchestrator over ``read_symbol_config`` (I/O + overlay) and ``merge_region_overrides``
    (region merge); call those directly when debugging which config was read vs. how it merged.
    """
    return merge_region_overrides(read_symbol_config(path), region)


def _get_symbol_ref(spec: dict[str, Any]):
    """Return the spec's source-symbol reference (``symbol``, falling back to legacy ``gdx``)."""
    ref = spec.get("symbol", spec.get("gdx"))
    if ref is None:
        raise KeyError(f"Symbol spec has neither 'symbol' nor 'gdx': {spec}")
    return ref


def _source_unit(spec: dict[str, Any], ref, resolved_name: str) -> str | None:
    """Resolve the source unit for a single-quantity spec.

    Supports a per-candidate ``units:`` list (parallel to the ``symbol:`` candidate list — the
    unit of whichever candidate actually resolved) or a scalar ``unit:``. Returns None if no
    unit is declared.
    """
    if "units" in spec:
        candidates = [ref] if isinstance(ref, str) else list(ref)
        return spec["units"][candidates.index(resolved_name)]
    return spec.get("unit")


def load_frame(loader: RemindLoader, spec: dict[str, Any]) -> pd.DataFrame:
    """Load the frame for one single-quantity symbol spec, applying its unit conversion.

    The source-symbol reference comes from ``symbol`` (backend-neutral; falls back to legacy
    ``gdx``). If the spec declares a source unit (``unit:`` or a per-candidate ``units:`` list)
    AND a target ``to_unit:``, the canonical ``value`` column is scaled by the central
    ``rpycpl.units`` factor on load — so conversions live in config + one table, not in code.
    A legacy ``unit_factor:`` scalar is still honoured. Use ``load_set`` for mixed-unit symbols.
    """
    ref = _get_symbol_ref(spec)
    resolved = loader.resolve_symbol(ref)
    df = loader.load_symbol(ref, rename_columns=spec.get("rename"))
    to_unit = spec.get("to_unit")
    src_unit = _source_unit(spec, ref, resolved)
    if to_unit is not None and src_unit is not None and "value" in df.columns:
        df = df.copy()
        df["value"] = df["value"] * unit_factor(src_unit, to_unit)
    if "unit_factor" in spec and "value" in df.columns:
        df = df.copy()
        df["value"] = df["value"] * spec["unit_factor"]
    return df


def load_set(loader: RemindLoader, spec: dict[str, Any]) -> pd.DataFrame:
    """Load a *mixed-unit set* symbol: one REMIND symbol whose ``index`` column selects several
    quantities with different units (e.g. ``pm_data`` indexed by ``char`` → lifetime/FOM/VOM).

    The spec's ``schema`` maps each index value to ``{parameter, unit, to_unit}``. Returns a long
    frame with a ``parameter`` column, ``value`` converted per row via the central units table,
    and a ``unit`` column set to the target unit. Index values not in the schema are dropped.
    """
    ref = _get_symbol_ref(spec)
    raw = loader.load_symbol(ref, rename_columns=spec.get("rename"))
    index = spec["index"]
    frames = []
    for key, sub in spec["schema"].items():
        part = raw[raw[index] == key].copy()
        if part.empty:
            continue
        if "to_unit" in sub:
            part["value"] = part["value"] * unit_factor(sub.get("unit", sub["to_unit"]), sub["to_unit"])
        part["parameter"] = sub["parameter"]
        part["unit"] = sub.get("to_unit", sub.get("unit"))
        frames.append(part)
    return pd.concat(frames, ignore_index=True) if frames else raw.iloc[0:0].assign(parameter=[], unit=[])
