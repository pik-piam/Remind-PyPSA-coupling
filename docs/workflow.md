# Plugging into a PyPSA workflow

This page is the hands-on guide for connecting a PyPSA model to REMIND through `rpycpl`. For
the conceptual map (what lives in the package vs the model, the full adapter interface, the
symbol/unit config schema) see **[Architecture](architecture.md)**.

Wiring a model up is two steps:

1. **Write an adapter** in the model repo — subclass `rpycpl.CouplingAdapter`, implement
   `build_config_overrides`, and override only the hooks that genuinely differ for the model.
2. **Write thin Snakemake rules** — each rule builds a `RemindLoader` + `load_symbol_specs()`,
   constructs the adapter, calls one method, and writes the output into the model's resource
   paths.

Everything model-specific (paths, scenarios, sectors) stays in the model's own
Snakemake/config; everything REMIND-side is inherited from the package.

---

## 1. Write the adapter

The whole PyPSA-Eur adapter is ~40 lines
(`pypsa-eur/scripts/remind/adapter_remind_eur.py`):

```python
from rpycpl.adapters.base import CouplingAdapter

class RemindEurAdapter(CouplingAdapter):
    def adjust_cost_efficiencies(self, eff):          # hook: EUR-only quirk
        eff = super().adjust_cost_efficiencies(eff)
        eff.loc[eff["technology"] == "btin", "value"] **= 2
        return eff

    def prepare_capacities(self, caps):               # hook: merge VRE variants, scale batteries
        ...

    def build_config_overrides(self):                 # abstract: the EUR config shape
        return {
            "scenario": {"planning_horizons": list(self.config.get("planning_horizons", []))},
            "co2_prices": self.build_co2_prices().to_dict(orient="records"),
        }
```

`build_config_overrides` is the only method you *must* implement — it returns the nested dict
of config keys whose values come from REMIND (not the user), to be merged onto the model's
config before the network is built. Everything else (CO₂ prices, demand downscaling, capacity
floors, cost extraction) is inherited. Override a hook only when the model genuinely differs;
see the [adapter interface table](architecture.md#the-adapter-interface) for which methods are
overridable and when.

---

## 2. Write thin Snakemake rules

A rule is a thin wrapper: build the loader + symbols + adapter, call one method, write the
output. For example `import_REMIND_capacities.py`:

```python
loader  = RemindLoader(snakemake.input["remind_data"])
symbols = load_symbol_specs()
adapter = RemindEurAdapter(loader=loader, symbols=symbols, region_map={},
                           config={"link_techs": LINK_TECHS}, remind_regions=mapped_regions)
capacities = adapter.determine_must_build_capacity(tech_map)
capacities.to_csv(snakemake.output["capacities"], index=False)
```

Keep the rule logic minimal — resolve inputs, call the adapter, write the file. Anything more
than that usually belongs in the adapter (if it's REMIND-side) or in the package (if it's
shared).

---

## A second consumer: PyPSA-China

PyPSA-China follows the same pattern, with its differences expressed as two overrides rather
than new code:

- `downscale_country_demand` adds `apply_historical_calibration` (config-driven, replacing
  the old hardcoded `*= 0.956` CHA→mainland scalar and the 2020/2025 load fixes).
- `build_config_overrides` emits the China config shape (run metadata + CO₂ + horizons).

Its symbol differences (CO₂ = `pm_taxCO2eq`, run-metadata scalars `c_expname` /
`c_model_version`) live in the `CHA:` block of `remind_symbols.yaml` — **not in code**. China
has no EUR-style VRE-variant/battery capacity techs, so it inherits the identity
`prepare_capacities` unchanged.

---

## Generalising to a new model (e.g. PyPSA-Earth)

Adding a consumer is deliberately small. Nothing in the package assumes a particular region
set, country list, or network model — those all arrive through constructor arguments.

1. **Write the adapter.** In the model repo, `class RemindEarthAdapter(CouplingAdapter)`.
   Implement `build_config_overrides()` for the model's config shape. That alone gives you the
   full default pipeline (CO₂, demand downscaling, capacity floors, costs).
2. **Override only what differs.** REMIND techs needing pre-processing → `prepare_capacities`;
   an efficiency quirk → `adjust_cost_efficiencies`; an extra demand step →
   `downscale_country_demand` (call `super()` first). If nothing differs, you write nothing
   else.
3. **Supply the mappings as data, not code.** Provide the region→country map (`read_region_map`
   handles the IAMC region-mapping CSV) and the tech→carrier CSV. Earth's wider country
   coverage is just a longer `region_map` and a longer SSP proxy table.
4. **Add symbol deltas if the export differs.** If Earth's REMIND export uses different symbol
   names, add an `overrides:` block (or a model-local overlay via `RPYCPL_SYMBOLS`) — no
   package edit.
5. **Add unit rows if it introduces new units.** One row per new `(from, to)` pair in
   `units.py`.
6. **Write thin Snakemake rules** that build the loader/symbols/adapter and call the methods.

Compatibility guardrails that keep this open: the loader is backend- and region-agnostic; the
downscaler takes its region map and proxies from the caller (no hardcoded region sets); the
transforms are pure and name-agnostic; and the adapter is the *only* model-specific seam.
Anything that would hardcode a region, country, or PyPSA-network assumption into the package
belongs in an adapter instead.

---

## Quick reference: a coupling step end to end

If you ever need to drive the layers directly (a script, a test, a notebook) rather than
through an adapter:

```python
from rpycpl.io import RemindLoader, load_symbol_specs
from rpycpl.io.remind_symbols import load_frame
from rpycpl.transforms.co2_prices import extract_co2_prices, convert_co2_prices

loader  = RemindLoader("REMIND2PyPSA.gdx")        # 1. open source (backend auto-detected)
symbols = load_symbol_specs(region=None)          # 2. resolve coupling names → symbols (+ overrides)
raw     = load_frame(loader, symbols["co2_price"]) # 3. load + unit-convert (tC→tCO2 here)
prices  = convert_co2_prices(                      # 4. transform (no double conversion)
    extract_co2_prices(raw, regions=regions, years=years),
    currency_factor=1.0, carbon_to_co2=False,
)
```

Inside a model you would normally let the **adapter** orchestrate steps 3–4
(`adapter.build_co2_prices()`), and the **Snakemake rule** own steps 1–2 and the write-out.
