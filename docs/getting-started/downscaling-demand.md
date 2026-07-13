# Downscaling demand

The IAM reports demand per sector, per year, and per multi-country region. PyPSA models need
country-level demand with hourly profiles. This requires a **downscaling** approach for both the
spatial and the temporal dimensions.

!!! info "Scope: demand only"
    Downscaling in `iampypsa` today covers **electricity demand only** — not capacities.
    Capacity targets stay at whatever region resolution the IAM reports them at; they're
    reconciled with the model's existing plant database directly instead, a different process
    covered in [Harmonising Capacities](harmonising-capacities.md).

## Spatial downscaling: region → country

This part is implemented in `iampypsa`. A model region like REMIND's `ECE` typically covers many
countries; a country-resolution PyPSA model needs the region's demand split across its members.

- **The region → country map** — `read_region_map()` reads the packaged region-mapping CSV
  (`data/remind/regions.csv`) and returns `{region: [country, ...]}` (or the reverse, depending
  on `source`/`target`).
- **Proxy shares** — the split isn't even; it's weighted by *proxies*: normalised population and
  GDP projections (`downscale/proxy.py`) for most sectors, and degree-day-weighted demand
  proxies for heating-sensitive sectors (`heatpump`, `resistive`) instead. A sector's
  `sector_weights` entry names which proxies to blend and in what proportion (e.g.
  `{gdp: 0.6, population: 0.4}`).
- **Applying the split** — `disaggregate_demand_to_country()` applies those shares to every
  `(year, region, sector)` row. Most integrations don't call this directly; they call
  `Coupler.downscale_country_demand()`, which reads the regional demand and disaggregates it in
  one step.

## Temporal downscaling: annual → hourly

This part is **not implemented in `iampypsa`** — it happens on the PyPSA-model side, consistent
with the general split: IAM-side logic lives in the package, model-side logic lives in the model.

The IAM supplies one annual total per (region, year, sector) — never a profile. `iampypsa`'s
spatial downscaling (above) turns this into one annual total per (country, year, sector); the
PyPSA model then turns *that* into an hourly timeseries by taking a demand *shape* from another
source and scaling it so the profile's annual sum matches that country-level annual total.

## Next

- [Harmonising Capacities](harmonising-capacities.md) — the equivalent reconciliation step for
  capacities, which does *not* go through this downscaling machinery.
- [Sector Coupling](sector-coupling.md)
