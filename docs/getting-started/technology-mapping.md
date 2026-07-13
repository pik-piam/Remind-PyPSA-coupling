# Technology mapping

Each PyPSA model needs to supply a technology mapping file that maps PyPSA's internal technology
names to `iampypsa`'s canonical names. This file also decides whether each cost parameter comes
from the IAM, from PyPSA's own default, or is fixed to a specific value. The IAM and the PyPSA
model typically won't agree on technologies, and even where they do, not every parameter should
necessarily be taken from the IAM — this information lives in a single YAML file.

## Processing

The technology mapping file is read by `iampypsa.io.technology_mapping`:

- `load_technology_parameters(path)` — parses the YAML, returned as `{"technologies": {...}}`.
- `iam_name(tech, spec)` — the IAM-side technology name an entry pulls values from (defaults to
  the model's own key if not given).
- `build_technology_sources(spec)` — expands one entry into a `{parameter: source}` map across
  the seven **standard parameters**: `investment`, `FOM`, `VOM`, `efficiency`, `lifetime`,
  `fuel`, `"CO2 intensity"`.

## The YAML schema

Each top-level key is one of the model's own technology/carrier names. Its value is either a
bare string (the source for *all seven* standard parameters), or a dict for finer control:

```yaml
# Bare-string shorthand: every parameter comes from the IAM.
gas-ocgt: {source: IAM}

# iam_name: the IAM's name for this technology differs from the model's own.
solar: {source: IAM, iam_name: solar-pv}

# overrides: per-parameter exceptions, each either "IAM", "PyPSA", or a fixed value.
biomass-igcc-ccs:
  source: IAM
  overrides:
    CO2 intensity: {value: 0, unit: "tCO2/MWh_th", comment: "carbon-negative in REMIND, but that makes PyPSA unbounded"}

# A technology that's entirely PyPSA-sourced, with one parameter zeroed out because
# the IAM's own cost for the primary technology already includes it.
offwind-ac-connection-submarine:
  source: PyPSA
  overrides:
    investment: {value: 0, unit: "USD/MW/km", comment: "Connection costs already included in REMIND offwind investment"}
```

## Where it's used

The same mapping serves two different consumers:

- **Cost sourcing** — `transforms/costs.py`: `build_iam_techdata` and `build_pypsa_techdata`
  split the model's baseline cost table and the IAM's cost table according to each technology's
  `build_technology_sources`, and `apply_overrides` merges them back together, IAM values
  overriding baseline values only where `source: IAM` (or an explicit override) says so.
- **Capacity-target technology selection** — `transforms/capacities.py`:
  `build_capacity_targets` and `build_capacity_reporting_technologies` use the mapping to decide
  which of the IAM's reported technologies should be summed into which of the model's carriers
  when building capacity targets (see [Harmonising Capacities](harmonising-capacities.md) for how
  those targets then get applied).

If a technology declares `source: IAM` (directly or via an override) for a parameter, but the
IAM's own output has no matching data for it, `build_iam_techdata` raises an error.

## Next

- [Downscaling Demand](downscaling-demand.md)
- [Harmonising Capacities](harmonising-capacities.md) — the mapping's other consumer, capacity
  reconciliation.
