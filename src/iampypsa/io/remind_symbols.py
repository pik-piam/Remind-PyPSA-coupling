"""Central REMIND symbol configuration: load logical→GDX/IAMC symbol maps and load frames.

Symbol definitions evolve, so they live in YAML (not code) and are *layered*:

1. the package default ships at ``iampypsa/data/remind_symbols_gdx.yaml`` (or ``…_iamc.yaml``),
   selected by ``backend`` — neither is the implicit default when an explicit backend is given;
2. a model/run may overlay its own YAML — passed as ``path=`` or via the ``RPYCPL_SYMBOLS``
   environment variable — which is **deep-merged on top** of the default, so the overlay only
   needs to list what differs (a new symbol, a renamed candidate, a region override).

``load_symbol_specs`` is split into debuggable steps: ``read_symbol_config`` (I/O + overlay →
raw ``{default, overrides}`` dict) and ``merge_region_overrides`` (pure per-logical-name merge
→ the flat map). Inspect either on its own when debugging.

Loading layer:

* ``load_frame``     — single-quantity symbol, addressed via ``symbol:`` key.
* ``load_set``       — mixed-unit set symbol (``index:``/``schema:`` shape).
* ``load_variable_set`` — many-variables-to-one-frame assembly (``variables:`` shape);
  only satisfiable by a loader whose source format exposes named-variable lookup (IAMC today).
* ``load_spec``      — dispatcher: picks by spec shape (``variables:`` →
  ``load_variable_set``; ``index:``/``schema:`` → ``load_set``; else → ``load_frame``).
  A ``variables:`` spec still requires an IAMC-backed loader; the dispatch is on spec shape,
  not a guarantee that every spec shape works with every backend.

``report_fallbacks`` returns a summary of all fallback declarations in a symbol map so
coverage gaps are inspectable without running a full coupling.
"""

from __future__ import annotations

import importlib.resources
import logging
import os
from os import PathLike
from typing import Any

import pandas as pd
import yaml

from iampypsa.io.loader import RemindLoader
from iampypsa.units import unit_factor

logger = logging.getLogger(__name__)

#: Environment variable holding a path to a symbol-config overlay (deep-merged onto the default).
SYMBOL_CONFIG_ENV = "RPYCPL_SYMBOLS"


def default_symbol_config_path(backend: str | None = None) -> Any:
    """Return the path to the packaged default symbol config for ``backend``.

    ``backend="gdx"`` → ``remind_symbols_gdx.yaml``;
    ``backend="iamc"`` → ``remind_symbols_iamc.yaml``;
    ``backend=None`` → ``remind_symbols_gdx.yaml`` (backward-compatible default).
    """
    if backend == "iamc":
        name = "remind_symbols_iamc.yaml"
    else:
        name = "remind_symbols_gdx.yaml"
    path = importlib.resources.files("iampypsa.data").joinpath(name)
    # Fall back to legacy remind_symbols.yaml if the gdx file is absent (shouldn't happen
    # but guards against stale installs that haven't run the rename yet).
    if not hasattr(path, "read_text"):
        return path
    try:
        path.read_text()
    except FileNotFoundError:
        if backend in (None, "gdx"):
            return importlib.resources.files("iampypsa.data").joinpath("remind_symbols.yaml")
    return path


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


def read_symbol_config(
    path: str | PathLike | None = None,
    *,
    backend: str | None = None,
) -> dict[str, Any]:
    """Read the raw symbol config (``{default, overrides}``), overlaying a user file if any.

    Always starts from the packaged default for ``backend``. An overlay file (``path``, else
    the ``RPYCPL_SYMBOLS`` env var) is deep-merged on top.
    """
    base_path = default_symbol_config_path(backend)
    base = yaml.safe_load(base_path.read_text())
    overlay_path = path if path is not None else os.environ.get(SYMBOL_CONFIG_ENV)  # noqa: E501
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


def load_symbol_specs(
    region: str | None = None,
    path: str | PathLike | None = None,
    *,
    backend: str | None = None,
) -> dict[str, Any]:
    """Return the resolved symbol map for ``backend`` and ``region``.

    ``region`` and ``path`` are positional for backward compatibility with existing callers.
    Pass ``backend=loader.backend`` as a keyword argument to select the GDX or IAMC config.
    ``backend=None`` (default) falls back to the GDX config.
    """
    return merge_region_overrides(read_symbol_config(path, backend=backend), region)


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

    Resolves ``symbol`` (falls back to legacy ``gdx``), then scales ``value`` via the central
    ``iampypsa.units`` factor when both a source unit (``unit:``/``units:``) and ``to_unit:``
    are declared (a legacy ``unit_factor:`` scalar is also honoured). Stamps the resolved unit
    onto a ``unit`` column, matching ``load_set``/``load_variable_set``, unless no unit is
    declared at all. Use ``load_set`` for mixed-unit symbols.
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
    unit = to_unit if to_unit is not None else src_unit
    if unit is not None and "value" in df.columns:
        df = df.copy()
        df["unit"] = unit
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


def load_variable_set(loader: RemindLoader, spec: dict[str, Any]) -> pd.DataFrame:
    """Load a *variable-set* spec: many IAMC variables → one token-labelled frame.

    IAMC-only (no GDX equivalent — GDX already carries a tech-domain column per symbol).
    ``variables:`` maps IAMC variable names to token labels; optional ``derived:`` declares
    linear combinations. Fallback tokens in ``spec['fallback']`` (``{token: {value, unit,
    reason}}``) are synthesised for every ``(year, region)`` when absent from the data.
    """
    from iampypsa.io.iamc import assemble_variable_set, read_iamc

    if loader.backend != "iamc":
        raise ValueError(
            f"load_variable_set requires an IAMC-backed loader, got {loader.backend!r}. "
            "Use load_frame or load_set for GDX specs."
        )

    mapping: dict[str, str] = spec["variables"]
    derived: dict[str, list] | None = spec.get("derived")
    label_col: str = spec.get("label_col", "technology")
    to_unit: str | None = spec.get("to_unit")

    # Collect all variable names (direct + derived components).
    direct_vars = set(mapping)
    derived_vars: set[str] = set()
    if derived:
        for terms in derived.values():
            derived_vars.update(v for _, v in terms)
    all_vars = list(direct_vars | derived_vars)

    df = read_iamc(loader.source, variables=all_vars)
    result = assemble_variable_set(df, mapping, label_col=label_col, derived=derived, to_unit=to_unit)

    # Synthesise rows for any fallback tokens absent from the loaded data.
    # A fallback entry must declare a ``value``; ``unit`` defaults to ``to_unit`` if omitted.
    fallback = spec.get("fallback", {})
    if fallback:
        present = set(result[label_col].unique()) if not result.empty else set()
        yr_reg = (
            result[["year", "region"]].drop_duplicates()
            if not result.empty
            else pd.DataFrame(columns=["year", "region"])
        )
        fallback_frames = []
        for token, fb in fallback.items():
            if token in present:
                continue
            logger.warning(
                "Token %r absent from mif variable set; using declared fallback: %s",
                token,
                fb.get("reason", "(no reason given)"),
            )
            if "value" not in fb:
                continue
            rows = yr_reg.copy()
            rows[label_col] = token
            rows["value"] = fb["value"]
            rows["unit"] = fb.get("unit", to_unit or "")
            fallback_frames.append(rows)
        if fallback_frames:
            result = pd.concat([result, *fallback_frames], ignore_index=True)

    return result


def load_spec(loader: RemindLoader, spec: dict[str, Any]) -> pd.DataFrame:
    """Dispatch to ``load_variable_set`` or ``load_frame`` based on the spec shape.

    Dispatches on spec *shape* (``variables:`` vs ``symbol:``), not on ``loader.backend`` —
    though a ``variables:`` spec still only works against a loader whose format supports it
    (currently IAMC; see ``load_variable_set``).
    """
    if "variables" in spec:
        return load_variable_set(loader, spec)
    return load_frame(loader, spec)


def report_fallbacks(symbols: dict[str, Any]) -> pd.DataFrame:
    """Return a summary DataFrame of all fallback declarations in a symbol map.

    Scans every spec in ``symbols`` for a ``fallback:`` block and returns
    ``[logical_name, token, value, reason]`` so coverage gaps are inspectable without a run.
    """
    rows = []
    for name, spec in symbols.items():
        if not isinstance(spec, dict):
            continue
        for token, fb in spec.get("fallback", {}).items():
            rows.append({
                "logical_name": name,
                "token": token,
                "value": fb.get("value"),
                "reason": fb.get("reason", ""),
            })
    return pd.DataFrame(rows, columns=["logical_name", "token", "value", "reason"])
