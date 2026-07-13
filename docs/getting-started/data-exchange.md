# Data exchange between IAMs and PyPSA

This page documents what data is exchanged between IAMs and PyPSA in principle.

## IAM → PyPSA (in active use)

Four pathway variables are exchanged from the IAM to PyPSA. Each has a distinct format.

| Variable | Dimensionality | What it is | Produced by |
|---|---|---|---|
| **Demand** | one value per (region, year, sector) | Annual sectoral electricity demand — a yearly total, **not** an hourly profile | `Coupler.build_regional_demand()` |
| **Costs** | several components per (region, technology, year) | Investment, FOM, VOM, efficiency, lifetime, fuel cost, CO2 intensity | `Coupler.extract_cost_parameters(year)` |
| **Capacities** | one value per (region, technology, year) | Installed-capacity targets from the IAM | `iampypsa.transforms.capacities.build_capacity_targets(...)` |
| **CO2 price** | one value per (region, year) | The regional carbon-price pathway | `Coupler.build_co2_prices()` |

A few things worth knowing about all four:

- All data comes in **tidy frames** — long-format tables with key columns like `region`, `year`,
  ... and a `value` + `unit` column.
- Units and the underlying IAM symbol names are never hardcoded in the transform functions —
  they're declared in a small YAML config (`remind_symbols_gdx.yaml` / `remind_symbols_mif.yaml`
  for the REMIND backends) and applied once, at load time.
- **Demand** is supplied as an *annual* total per sector and IAM region. This is then downscaled
  to country level (see [Downscaling Demand](downscaling-demand.md)), based on SSP projections
  for GDP and population as well as heating degree days (HDDs) and cooling degree days (CDDs).
  The temporal downscaling into an hourly demand profile happens on the PyPSA side, such that the
  profile's annual sum matches the IAM's annual sectoral total.
- **Costs** can be set by the IAM, by the PyPSA default, or to a fixed value (see
  [Technology Mapping](technology-mapping.md)). A separate `Coupler.discount_rates(year)` method
  supplies the per-region discount rate, merged into the cost table alongside these components.
  For consistency with the IAM, additional technologies may need to be added on the PyPSA side.
- **Capacities** aren't downscaled the way demand is — see the scope note at the top of
  [Downscaling Demand](downscaling-demand.md). Reconciling them with a model's existing plant
  database is its own step, [Harmonising Capacities](harmonising-capacities.md).

## PyPSA → IAM (**not currently in use**)

A reverse direction will be implemented in the future to enable bidirectional iterative coupling.
The main parameters planned for exchange from PyPSA to the IAM are: capacity factors, market
values, sectoral prices, battery and hydrogen storage capacities, and grid transmission and
firm-capacity constraints.

**This is not implemented in `iampypsa` yet.**

## Next

- [How the Package Works](integrating-a-model.md) — the general pattern for wiring IAM variables into the PyPSA workflow.
