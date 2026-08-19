"""Turn a quantity spec into a canonical, unit-converted frame.

``load_quantity`` is the one entry point callers need: it classifies the spec's *shape* and
routes it to the module owning that data model. Only the plain ``symbol:`` shape is loaded
here, because it is genuinely cross-format — the shape-specific loaders live with their format
(``formats.gams.load_indexed``, ``formats.iamc.load_variables``).

Unit conversion happens here, once, at the load seam: :mod:`iampypsa.quantities.conversion`
resolves which unit a spec/frame declares, then applies the central ``iampypsa.units`` factor.
"""

import logging
from typing import Any

import pandas as pd

from iampypsa.formats import gams, iamc
from iampypsa.quantities.conversion import convert_column, resolve_source_unit
from iampypsa.quantities.schema import find_spec_shape, get_quantity_ref

logger = logging.getLogger(__name__)

#: Spec shape → (loader, backends that can serve it), declared by the format modules.
SHAPE_LOADERS = {
    shape: (load, module.BACKENDS)
    for module in (gams, iamc)
    for shape, load in module.SPEC_SHAPES.items()
}


def load_quantity(loader, spec: dict[str, Any]) -> pd.DataFrame:
    """Load one quantity spec, dispatching on its shape.

    Dispatches on spec *shape*, not on ``loader.backend`` — but a shape only one data model can
    serve (``variables:`` for IAMC, ``index:``/``schema:`` for GAMS) raises against any other
    backend rather than silently ignoring the keys it can't honour.
    """
    shape = find_spec_shape(spec)
    if shape not in SHAPE_LOADERS:
        return load_simple(loader, spec)
    load, backends = SHAPE_LOADERS[shape]
    if loader.backend not in backends:
        raise ValueError(
            f"A {shape!r} spec needs a {sorted(backends)} loader, got {loader.backend!r}."
        )
    return load(loader, spec)


def load_simple(loader, spec: dict[str, Any]) -> pd.DataFrame:
    """Load the frame for one single-quantity ``symbol:`` spec, applying its unit conversion.

    Resolves the name, then scales ``value`` via the central ``iampypsa.units`` factor when both
    a source unit and ``to_unit:`` are declared. The source unit is read live from the data when
    available (IAMC's own ``unit`` column), else from the spec's ``unit:``/``units:`` (GAMS
    carries no per-row unit info). Stamps the resolved unit onto a ``unit`` column, unless no
    unit is available at all.

    An optional ``filter: {column: value}`` drops rows that don't match — e.g. selecting a
    single GAMS domain slice (``rlf: 1``) out of a symbol that carries extra dimensions.
    """
    ref = get_quantity_ref(spec)
    resolved = loader.resolve(ref)
    # Pass the resolved name, not the candidate list — read would otherwise re-resolve.
    df = loader.read(resolved, rename_columns=spec.get("rename"))
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


def report_fallbacks(specs: dict[str, Any]) -> pd.DataFrame:
    """Return a summary DataFrame of all fallback declarations in a quantity-spec map.

    Scans every spec for a ``fallback:`` block and returns ``[logical_name, token, value,
    reason]`` so coverage gaps are inspectable without running a full coupling.
    """
    rows = []
    for name, spec in specs.items():
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
