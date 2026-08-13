"""Central REMIND symbol configuration: load logical→GDX/IAMC symbol maps and load frames.

1. the package default ships at ``iampypsa/data/remind_symbols_gdx.yaml`` (or ``…_mif.yaml``),
   selected by ``backend``.
2. a model/run may overlay its own YAML — passed as ``path=`` — which is **deep-merged on top**
   of the default, so the overlay only needs to list what differs (a new symbol, a renamed
   candidate, a region override).

Loading layer:

* ``load_frame``     — single-quantity symbol, addressed via ``symbol:`` key. GDX and IAMC.
* ``load_set``       — mixed-unit set symbol (``index:``/``schema:`` shape). GDX only.
* ``load_variable_set`` — many-variables-to-one-frame assembly (``variables:`` shape).
  IAMC only — raises if given a GDX-backed loader.
* ``load_spec``      — dispatcher: picks by spec shape (``variables:`` →
  ``load_variable_set``; ``index:``/``schema:`` → ``load_set``; else → ``load_frame``).

Unit conversion (resolving which unit a spec/frame declares, then applying the central
``iampypsa.units`` factor) is generic and lives in ``iampypsa.io.conversion``.

``report_fallbacks`` returns a summary of all fallback declarations in a symbol map so
coverage gaps are inspectable without running a full coupling.
"""

import importlib.resources
import logging
from importlib.resources.abc import Traversable
from os import PathLike
from typing import Any

import pandas as pd
import yaml

from iampypsa.io.conversion import convert_column, resolve_source_unit
from iampypsa.io.loader import Backend, RemindLoader, SymbolRef

logger = logging.getLogger(__name__)

#: Packaged default symbol config per backend.
_BACKEND_CONFIGS = {
    "gdx": "remind_symbols_gdx.yaml",
    "iamc": "remind_symbols_mif.yaml",
}


def default_symbol_config_path(backend: Backend) -> Traversable:
    """Return the path to the packaged default symbol config for ``backend``.

    ``backend`` is required and must be known. There is deliberately no GDX fallback: it made a
    typo, or a forgotten argument on an IAMC run, resolve GDX symbol names against a mif — which
    surfaces far from the cause, or not at all. Pass ``backend=loader.backend``.

    Args:
        backend: ``"gdx"`` or ``"iamc"``.

    Raises:
        ValueError: If ``backend`` is not a known backend.
    """
    if backend not in _BACKEND_CONFIGS:
        raise ValueError(
            f"Unknown backend {backend!r}; expected one of {sorted(_BACKEND_CONFIGS)}."
        )
    return importlib.resources.files("iampypsa.data").joinpath(_BACKEND_CONFIGS[backend])


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
    backend: Backend,
) -> dict[str, Any]:
    """Read the raw symbol config (``{default, overrides}``), overlaying a user file if any.

    Always starts from the packaged default for ``backend``. An overlay file (``path``) is
    deep-merged on top.

    Args:
        path: Overlay YAML to deep-merge onto the packaged default.
        backend: ``"gdx"`` or ``"iamc"`` — required, see ``default_symbol_config_path``.
    """
    base = yaml.safe_load(default_symbol_config_path(backend).read_text())
    if path:
        with open(path) as f:
            base = _merge_config(base, yaml.safe_load(f))
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
    backend: Backend,
) -> dict[str, Any]:
    """Return the resolved symbol map for ``backend`` and ``region``.

    Args:
        region: IAM region whose ``overrides:`` block wins over ``default:``; ``None`` for
            the defaults alone. Positional, for existing callers.
        path: Overlay YAML to deep-merge onto the packaged default. Positional, as above.
        backend: ``"gdx"`` or ``"iamc"`` — required. Pass ``backend=loader.backend``.
    """
    return merge_region_overrides(read_symbol_config(path, backend=backend), region)


def _derive_symbol_ref(spec: dict[str, Any]) -> SymbolRef:
    """Return the spec's source-symbol reference."""
    ref = spec.get("symbol")
    if ref is None:
        raise KeyError(f"Symbol spec has no 'symbol': {spec}")
    return ref


def load_frame(loader: RemindLoader, spec: dict[str, Any]) -> pd.DataFrame:
    """Load the frame for one single-quantity symbol spec, applying its unit conversion.

    Resolves ``symbol``, then scales ``value`` via the central
    ``iampypsa.units`` factor when both a source unit and ``to_unit:`` are declared. The source
    unit is read live from the data when available (mif's own ``Unit`` column), else from the
    spec's ``unit:``/``units:`` (GDX has no per-row unit info). Stamps the resolved unit onto a
    ``unit`` column, matching ``load_set``/``load_variable_set``, unless no unit is available at
    all. Use ``load_set`` for mixed-unit symbols.

    An optional ``filter: {column: value}`` drops rows that don't match — e.g. selecting a
    single GAMS domain slice (``rlf: 1``) out of a symbol that carries extra dimensions.
    """
    ref = _derive_symbol_ref(spec)
    resolved = loader.resolve_symbol(ref)
    # Pass the resolved name, not the candidate list — load_symbol would otherwise re-resolve.
    df = loader.load_symbol(resolved, rename_columns=spec.get("rename"))
    for col, value in spec.get("filter", {}).items():
        # GAMS domain columns are categorical/string labels even for numeric-looking values
        # (e.g. rlf: 1) -- compare as strings so a plain int in the spec still matches.
        df = df[df[col].astype(str) == str(value)]
    to_unit = spec.get("to_unit")
    src_unit = resolve_source_unit(spec, ref, resolved, df)
    df = convert_column(df, "value", src_unit, to_unit)
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
    ref = _derive_symbol_ref(spec)
    raw = loader.load_symbol(ref, rename_columns=spec.get("rename"))
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


def load_variable_set(loader: RemindLoader, spec: dict[str, Any]) -> pd.DataFrame:
    """Load a *variable-set* spec: many IAMC variables → one token-labelled frame.

    IAMC-only (no GDX equivalent — GDX already carries a tech-domain column per symbol).
    ``variables:`` maps IAMC variable names to token labels; optional ``derived:`` declares
    linear combinations. Fallback tokens in ``spec['fallback']`` (``{token: {value, unit,
    reason}}``) are synthesised for every ``(year, region)`` when absent from the data.
    """
    from iampypsa.io.iamc import build_variable_set, read_iamc

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
    result = build_variable_set(df, mapping, label_col=label_col, derived=derived, to_unit=to_unit)

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


def rename_technologies(
    df: pd.DataFrame,
    names: dict[str, str] | None,
    col: str = "technology",
    *,
    on_missing: str = "warn",
) -> pd.DataFrame:
    """Rename raw source-model technology tokens to the canonical vocabulary.

    ``names`` is the ``technology_names`` token → canonical-name block; empty/absent is a
    no-op. Unmapped values are kept as-is, per ``on_missing``: ``"warn"`` (default), ``"raise"``,
    or ``"ignore"``.
    """
    if not names or col not in df.columns:
        return df
    values = df[col].astype(str)
    missing = sorted(set(values.unique()) - set(names))
    if missing:
        if on_missing == "raise":
            raise KeyError(f"Technologies without a technology_names entry: {missing}")
        if on_missing == "warn":
            logger.warning(
                "Technologies without a technology_names entry (kept as-is): %s", missing
            )
    out = df.copy()
    out[col] = values.map(lambda t: names.get(t, t))
    return out


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


def build_capacity_reporting_technologies() -> set[str]:
    """Return every canonical technology REMIND reports installed capacity for.

    Reads the ``capacity`` spec's ``variables:``/``derived:`` tokens (``hydro`` is included there).
    """
    cap_spec = read_symbol_config(backend="iamc")["default"]["capacity"]
    return set(cap_spec.get("variables", {}).values()) | set(cap_spec.get("derived", {}))
