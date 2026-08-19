# Data exchange between IAMs and PyPSA

This page provides background information regarding the data that can be exchanged between IAMs and PyPSA.

## IAM → PyPSA (in active use)

Four pathway variables are exchanged from the IAM to PyPSA. Each has a distinct format.

| Variable | Dimensionality | What it is | Produced by |
|---|---|---|---|
| **Demand** | one value per (region, year, sector) | Annual sectoral electricity demand — a yearly total, **not** an hourly profile | `Coupler.build_regional_demand()` |
| **Costs and technology data** | several components per (region, technology, year) | Investment, FOM, VOM, efficiency, lifetime, fuel cost, CO2 intensity | `Coupler.extract_cost_parameters(year)` |
| **Capacities** | one value per (region, technology, year) | Installed-capacity targets from the IAM | `Coupler.get_capacities(...)` |
| **CO2 price** | one value per (region, year) | The regional carbon-price pathway | `Coupler.build_co2_prices()` |

A few things worth knowing about all four:

- All data comes in long-format tables with key columns like `region`, `year`,
  ... and a `value` + `unit` column.
- Units and the underlying IAM symbol names are declared in a YAML config
  (e.g. `models/remind/quantities_gdx.yaml` / `quantities_mif.yaml`) and applied
  once, at load time.
- **Costs and technical parameters** can be sourced by the IAM, by the PyPSA cost table, or set
  directly to a user-set value. This is governed by a `yaml` mapping (see
  [Technology mapping](technology-mapping.md)). The regional discount rate can be read directly
  from supported IAMs via `Coupler.build_discount_rates(year)` and is added to the
  techno-economics table used by the coupled PyPSA.
- **Currency conversion and discount rates**: monetary values are converted from the IAM's base
  currency (e.g. USD2017 for REMIND) to the PyPSA model's currency (e.g. EUR2015 for PyPSA-Eur) via the
  `currency_factor` parameter (default is `1.0`). This factor is set on the `Coupler`'s `config`
  (e.g. a `currency_factor` key in the model's own coupling config, such as
  `config.remind_de.yaml`) — not in the technology mapping — and is *not* intended for inflation
  correction or other currency-year adjustments.
- **Demand** is supplied as an *annual* total per sector and IAM region. This is then downscaled
  to country level (see [Downscaling demand](downscaling-demand.md)), based on SSP projections
  for GDP and population as well as heating degree days (HDDs) and cooling degree days (CDDs).
  The temporal downscaling into an hourly demand profile happens on the PyPSA side, such that the
  profile's annual sum matches the IAM's annual sectoral total.
- **Capacities** aren't downscaled the way demand is — see the scope note at the top of
  [Downscaling demand](downscaling-demand.md). Reconciling them with a model's existing plant
  database is its own step, see [Harmonising capacities](harmonising-capacities.md).

## PyPSA → IAM (**not currently in use**)

A reverse direction will be implemented in the future to enable bidirectional iterative coupling.
The main parameters planned for exchange from PyPSA to the IAM are: capacity factors, market
values, sectoral prices, battery and hydrogen storage capacities, and grid transmission and
firm-capacity constraints.

**This is not implemented in `iampypsa` yet.**

## Next

- [Technology mapping](technology-mapping.md) — how costs and capacities get mapped onto PyPSA's own carrier/technology names.
