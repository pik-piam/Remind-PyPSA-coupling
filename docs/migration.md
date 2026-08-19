# Migrating a consumer to the restructured `iampypsa`

The 2026-08-19 restructure moved every module and renamed the loader API. **There are no
deprecation shims** — a consumer pinned to the old layout keeps working until it upgrades, and
breaks the moment it does. Pin the old version, migrate, unpin.

Nothing about *what* the package computes changed. The golden reference CSVs
(`co2_price`, `sectoral_load`, `sectoral_load_country`, `installed_capacities`,
`costs_raw_overwritten`) are byte-identical across the restructure, so a correct migration
produces identical output. If your numbers move, the migration is wrong — not the package.

---

## 1. The mechanical part

Most of the diff is renames. These are safe to apply with `sed` across your `scripts/`:

| Old | New |
|---|---|
| `RemindLoader` | `IamLoader` |
| `load_symbol_specs` | `load_quantity_specs` |
| `read_symbol_config` | `read_quantity_config` |
| `default_symbol_config_path(backend=…)` | `models.get_default_config_path(model, backend)` |
| `load_frame` / `load_set` / `load_spec` / `load_variable_set` | `load_quantity` (one entry point) |
| `loader.resolve_symbol(...)` | `loader.resolve(...)` |
| `loader.load_symbol(...)` | `loader.read(...)` |
| `loader.load_scalar(...)` | `loader.read_scalar(...)` |
| `loader.list_symbols()` | `loader.list_names()` |
| `RemindLoader.detect_backend(path)` | `formats.detect_backend(path)` |
| `iam_name(tech, spec)` | `get_iam_name(tech, spec)` |
| `Coupler(..., symbols=…)` | `Coupler(..., quantities=…)` |
| `coupler.symbols` | `coupler.quantities` |
| `SymbolRef` | `QuantityRef` |

```bash
sed -i \
  -e 's/\bRemindLoader\b/IamLoader/g' \
  -e 's/\bload_symbol_specs\b/load_quantity_specs/g' \
  -e 's/\bread_symbol_config\b/read_quantity_config/g' \
  -e 's/\bload_frame\b/load_quantity/g' \
  -e 's/\bload_set\b/load_quantity/g' \
  -e 's/\bload_spec\b/load_quantity/g' \
  -e 's/\bload_variable_set\b/load_quantity/g' \
  -e 's/\bresolve_symbol\b/resolve/g' \
  -e 's/\biam_name(/get_iam_name(/g' \
  scripts/import_REMIND_*.py scripts/downscale_REMIND_demand.py
```

!!! warning "Two traps in that `sed`"
    `iam_name` is also a **YAML key** in every technology-mapping file. The `(` in the pattern
    above keeps it to call sites — check the diff anyway.
    `load_symbol` → `read` is *not* included: it would also rewrite `read_gdx_symbol` and
    `list_gdx_symbols`. Do those by hand.

## 2. Import paths

`io/` and `couplers/` no longer exist. `from iampypsa.io import X` has no single replacement —
the package was four unrelated things under one label, which is why it was split.

| Old import | New import |
|---|---|
| `from iampypsa.io import RemindLoader` | `from iampypsa import IamLoader` |
| `from iampypsa.io.loader import Backend, SymbolRef` | `from iampypsa.formats import Backend` / `from iampypsa.loader import QuantityRef` |
| `from iampypsa.io.remind_symbols import load_symbol_specs` | `from iampypsa.quantities import load_quantity_specs` |
| `from iampypsa.io.remind_symbols import load_frame, load_spec` | `from iampypsa.quantities import load_quantity` |
| `from iampypsa.io.remind_symbols import rename_technologies, report_fallbacks` | `from iampypsa.quantities import rename_technologies, report_fallbacks` |
| `from iampypsa.io.remind_symbols import merge_region_overrides` | `from iampypsa.quantities import merge_region_overrides` |
| `from iampypsa.io import load_technology_parameters` | `from iampypsa.quantities import load_technology_parameters` |
| `from iampypsa.io import iam_name, build_technology_sources` | `from iampypsa.quantities.schema import get_iam_name, build_technology_sources` |
| `from iampypsa.io.technology_mapping import STANDARD_PARAMETERS` | `from iampypsa.quantities.schema import STANDARD_PARAMETERS` |
| `from iampypsa.io import read_gdx_symbol, read_gdx_scalar, list_gdx_symbols` | `from iampypsa.formats.gdx import …` |
| `from iampypsa.io import read_iamc, list_iamc_variables` | `from iampypsa.formats.iamc import …` |
| `from iampypsa.io.iamc import build_variable_set` | `from iampypsa.formats.iamc import build_variable_set` |
| `from iampypsa.io.iamc import parse_currency_year` | `from iampypsa.units import parse_currency_year` |
| `from iampypsa.io import read_ssp_data, fetch_ssp_data, fetch_ssp_variable` | `from iampypsa.reference import …` (or `reference.ssp`) |
| `from iampypsa.io import read_degree_days` | `from iampypsa.reference import read_degree_days` |
| `from iampypsa.io import build_capacity_reporting_technologies` | `from iampypsa.models.remind import build_capacity_reporting_technologies` |
| `from iampypsa.couplers import Coupler` / `from iampypsa.couplers.base import Coupler` | `from iampypsa import Coupler` |
| `from iampypsa.couplers import RemindGdxCoupler, RemindIamcCoupler` | `from iampypsa.models.remind import …` |
| `from iampypsa.couplers.remind import read_region_map` | `from iampypsa.models.remind import read_region_map` |

**Unchanged:** every `iampypsa.transforms.*` and `iampypsa.downscale.*` import path and
signature, except that `downscale.Downscaler` / `ProportionalDownscaler` were **deleted** (dead
code — nothing called them; `disaggregate_demand_to_country` never did).

## 3. The part that isn't a rename

Seven changes need a human:

**a. `open_coupler` replaces the backend/coupler pairing.** This is the actual win — the stanza
every script repeated:

```python
# before
from iampypsa.couplers import RemindGdxCoupler, RemindIamcCoupler
from iampypsa.io import RemindLoader
from iampypsa.io.remind_symbols import load_symbol_specs

REMIND_COUPLERS = {"gdx": RemindGdxCoupler, "iamc": RemindIamcCoupler}

loader = RemindLoader(snakemake.input.remind_data)
symbols = load_symbol_specs(region, backend=loader.backend)
coupler = REMIND_COUPLERS[loader.backend](
    loader, symbols, region_map={region: [region]}, config={}, model_regions=[region],
)
```

```python
# after
from iampypsa import open_coupler

coupler = open_coupler(
    snakemake.input.remind_data,
    model="remind",
    region=region,                      # was the 1st positional arg of load_symbol_specs
    region_map={region: [region]},
    config={},
    model_regions=[region],
)
```

Direct construction still works if you need the loader or specs separately:
`RemindGdxCoupler(loader, quantities, region_map, config)` — note the second argument is
positional and now called `quantities`.

**b. `open_coupler` raises when it can't tell which regions to couple.** If both `region_map`
and `model_regions` are empty, every builder would silently return an empty frame. Pass at least
one. Omitting `region_map` entirely now loads REMIND's packaged region → country map, which is
what `read_region_map(source="model_region", target="country")` returned.

**c. Indexed specs now actually dispatch.** `load_spec` never routed `index:`/`schema:` specs —
they fell through to `load_frame`, which ignored both keys and returned unsplit rows, so callers
had to know to call `load_set` by hand. `load_quantity` routes them. **If you called `load_set`
explicitly, `load_quantity` gives the identical frame.** If you called `load_spec` on such a spec
and worked around the unsplit result downstream, that workaround is now wrong. In the shipped
REMIND configs only `tech_data` (GDX) has this shape.

**d. A spec shape the backend can't serve now raises both ways.** Previously a `variables:` spec
on a GDX loader raised, but an `index:`/`schema:` spec on an IAMC loader passed silently. Both
raise now, with the shape and the backends that can serve it in the message.

**e. `build_capacity_reporting_technologies()` takes its specs.** It used to hardcode
`backend="iamc"` internally and ignore any overlay, so it silently returned the wrong set on a
GDX run:

```python
# before                                     # after
build_capacity_reporting_technologies()      build_capacity_reporting_technologies(
                                                 load_quantity_specs(backend="iamc")
                                             )
```
Pass the IAMC specs to keep the old behaviour — the tokens live in that config's
`capacity.variables`/`derived` block, and the GDX config has no equivalent.

**f. Packaged data moved.** If you referenced the YAMLs by path:
`iampypsa/data/remind_symbols_gdx.yaml` → `iampypsa/models/remind/quantities_gdx.yaml`,
`…_mif.yaml` → `quantities_mif.yaml`, `iampypsa/data/remind/regions.csv` →
`iampypsa/models/remind/regions.csv`. Prefer `models.get_default_config_path("remind", backend)`
over a hand-built path. **The YAML schema itself is unchanged** — your overlay files and
`overrides:` blocks need no edits.

**g. A `Coupler` subclass implements `build_cost_parameters`, not `extract_cost_parameters`.**
Only relevant if you subclass `Coupler` in your model repo — consumers that merely *call* it are
unaffected. `extract_cost_parameters` is now a concrete template that applies the currency
factor, the technology rename and the fuel-price broadcast to whatever the hook returns, so
those three can no longer be forgotten:

```python
# before                              # after
class MyCoupler(Coupler):             class MyCoupler(Coupler):
    def extract_cost_parameters(          def build_cost_parameters(
        self, year                            self, year
    ):                                    ):
        df = ...                              return ...   # raw rows; no currency,
        df = apply_currency_factor(...)                    # no rename, no broadcast
        df = rename_technologies(...)
        df = broadcast_fuel_prices(...)
        return df
```

Keep overriding `extract_cost_parameters` and you silently skip all three — the base class emits
a warning at import time saying so. If your subclass derives its `tech_fuel_map` from the source
rather than the YAML, override `get_tech_fuel_map()`; if it needs to drop technologies with no
`technology_names` entry, set `drop_unmapped_technologies = True`.

## 4. Per-consumer notes

### PyPSA-China-PIK

The current coupling work is on **`update_remind_coupling`** (2026-07-14, "attempt update to
latest dev"); `main` and `develop` are still two generations back on `rpycpl` + the
`ETL_REGISTRY`/`Transformation` API and are a separate, larger job.

On `update_remind_coupling` the affected files are:

| File | What it uses |
|---|---|
| `workflow/scripts/iam_coupling/import_REMIND_demand.py` | `REMIND_COUPLERS` pairing → `open_coupler` (3a) |
| `workflow/scripts/iam_coupling/import_REMIND_config.py` | same pairing |
| `workflow/scripts/iam_coupling/import_REMIND_costs.py` | same pairing, + `build_technology_sources`, `load_technology_parameters` |
| `workflow/scripts/iam_coupling/import_REMIND_capacities.py` | `RemindLoader`, `load_symbol_specs`, `rename_technologies`, `prepare_capacities` |
| `workflow/scripts/iam_coupling/capacity_harmonization.py` | `build_technology_sources`, `iam_name` |
| `workflow/scripts/iam_coupling/downscale_REMIND_demand.py` | `read_region_map`, `read_degree_days` |
| `workflow/scripts/fetch_ssp_data.py` | `iampypsa.io.ssp.fetch_ssp_data` |

!!! danger "One import there is already broken, before this migration"
    `import_REMIND_capacities.py` does
    `from iampypsa.transforms.capacities import prepare_capacities` and calls
    `prepare_capacities(loader, symbols)`. There is no such function — `prepare_capacities` is a
    **`Coupler` method** taking no arguments. That branch predates the move. Fix it as
    `open_coupler(...).prepare_capacities()`, which also removes the need to build the loader and
    specs by hand. Do not treat this as restructure fallout.

### PyPSA-Eur

**I could not survey this properly.** The only checkout in the workspace
(`/workspace/pypsa-eur-aod/pypsa-eur`) is not a git repository and is two generations old — it
imports `rpycpl.adapters`, `RemindGdxAdapter`, `transforms.mapping`, `build_capacity_targets`.
Everything below is therefore about *structure*, not verified line numbers.

The scripts in scope are `scripts/import_REMIND_{co2price,costs,capacities,demand,hydro,config}.py`,
`downscale_REMIND_demand.py`, `add_electricity_sector_REMIND.py`,
`installed_capacity_constraints_REMIND.py`, wired by `rules/REMIND_coupling.smk`.

Migrate in this order, because it front-loads the risk:

1. `import_REMIND_co2price.py` — the simplest; proves `open_coupler` + the golden CO2 frame.
2. `downscale_REMIND_demand.py` — touches `downscale`/`reference` only, which barely changed.
3. `import_REMIND_capacities.py` — exercises `prepare_capacities`/`get_capacities`.
4. `import_REMIND_costs.py` — chains six transforms and carries the model-specific `btin`
   squaring. If the facade reads badly anywhere, it will be here.

### Which import name?

This repo's import name is `iampypsa`; consumers have been described as using `iam2pypsa`.
**That discrepancy is unresolved and this migration does not settle it.** Resolve it before, not
during — a third rename on top of a restructure makes both harder to review.

## 5. Verification

1. `python -c "import iampypsa; print(iampypsa.__all__)"` → the five facade names.
2. `grep -rn "iampypsa\.io\|iampypsa\.couplers\|RemindLoader\|load_symbol_specs\|load_frame\|load_set\|load_spec\|load_variable_set" scripts/` → no hits.
3. Run each migrated rule standalone via its `mock_snakemake(...)` block.
4. **Diff the outputs against a pre-migration run, not against expectations.** Same input file,
   same scenario, same years: `sectoral_load.csv`, `sectoral_load_country.csv`,
   `installed_capacities.csv`, `co2_price.csv` and the cost table must be identical. Any
   difference is a migration bug, since the package's own golden files did not move.
5. Only then run the full DAG.

## 6. What did not change

Worth knowing so you don't go looking:

- Every `Coupler` method you *call* — `build_regional_demand`, `extract_cost_parameters`,
  `build_co2_prices`, `downscale_country_demand`, `build_discount_rates`,
  `prepare_capacities`, `get_capacities` — same names, signatures and semantics.
  (One exception for anyone who *subclasses* `Coupler`: see below.)
- `tech_map` is still an **argument** to `get_capacities` — the carrier vocabulary stays
  PyPSA-side.
- The whole `transforms/` API, and `downscale.disaggregate_demand_to_country` /
  `build_proxy_shares` / `build_demand_proxy_from_dd`.
- The quantity-spec YAML schema, `overrides:` layering, candidate lists, `postprocessing:`.
- `UNIT_CONVERSIONS` and the rule that conversion happens once, at the load seam.
- `backend=` is still a required keyword with no default on `load_quantity_specs`.
- `Coupler` is still **not** an ABC.
