# How the package works

This page explains the general pattern for wiring a PyPSA model up to `iampypsa` — the shape of
the workflow, the dimensions you need to think about, and which package function
produces which output. For a concrete, currently-working example,
look at [`pypsa-eur-iam`](https://github.com/pik-piam/pypsa-eur-iam).

## The general pattern

A PyPSA model's own Snakemake workflow reads the IAM's output **once**, near the start of the
workflow, and turns it into files the rest of the workflow consumes like any other input.

The general approach is:

1. **`iampypsa`** does the IAM-side work: reading the source file, converting units, and
   producing tidy frames for demand, costs, capacities, and CO2 prices. This can be called
   either through a `Coupler` subclass (`RemindGdxCoupler` / `RemindIamcCoupler` today), or
   through the `io`/`transforms` functions directly for a specific step.
2. **PyPSA's Snakemake rules** do everything model-specific: file paths, model configuration, and
   how the inputs from `iampypsa` are used in the network (buses, generators, links, loads).

## Additional wildcards

PyPSA should be run in `overnight` foresight mode, *not* `myopic`, since the IAM already handles
the pathway dimension across years.

A new wildcard is therefore typically required for the year dimension — see e.g.
[`pypsa-eur-iam`](https://github.com/pik-piam/pypsa-eur-iam)'s `year_REMIND`.

Further wildcards may be necessary for iterations once bidirectional/iterative coupling is
enabled.

## Steps to follow

Regardless of the PyPSA model, the following steps should always be followed:

1. **Read regional sectoral demand** — `Coupler.build_regional_demand()`.
2. **Downscale demand to country level** — `Coupler.downscale_country_demand()`. See
   [Downscaling Demand](downscaling-demand.md).
3. **Read capacity targets** — `iampypsa.transforms.capacities.build_capacity_targets(...)`.
4. **Read CO2 prices** — `Coupler.build_co2_prices()`.
5. **Read costs** — `Coupler.extract_cost_parameters(year)`.
6. **Adjust the model's existing plant database** to match the capacity targets from step 3 —
   this step is model-specific; see [Harmonising Capacities](harmonising-capacities.md).

Each step is a thin Snakemake rule: build a `RemindLoader` (or reuse one), resolve the symbol
config, call the one method or function that step needs, and write the result to the model's own
resource path. Keep the rule itself minimal — if a rule is doing more than "load, call, write",
that logic likely belongs either in the model's own Snakemake/coupling code (if it's model-side)
or in `iampypsa` (if it's genuinely IAM-side and shared).

## Reference implementation: `pypsa-eur-iam`

`pypsa-eur-iam` showcases an implementation of this approach —
`Snakefile_REMIND` sets up the workflow, `rules/REMIND_coupling.smk` contains all coupling rules,
and `scripts/remind/` holds the per-step scripts.

## Next

- [Technology Mapping](technology-mapping.md) — how costs and capacities get mapped onto PyPSA's own carrier/technology names.
- [Downscaling Demand](downscaling-demand.md) — turning regional annual demand into country-level demand.
- [Harmonising Capacities](harmonising-capacities.md) — reconciling capacity targets with an
  existing power plant database.
- [Sector Coupling](sector-coupling.md) — the stylised approach used for sector-specific demand.
