# Downscaling demand

The IAM reports demand per sector, per year, and per multi-country region. PyPSA models need
country-level demand with hourly profiles. This requires a **downscaling** approach for both the
spatial and the temporal dimensions.

!!! info "Scope: demand only"
    Downscaling in `iampypsa` today covers **electricity demand only** — not capacities.
    Capacity targets stay at whatever region resolution the IAM reports them at; they're
    reconciled with the model's existing plant database directly instead, a different process
    covered in [Harmonising capacities](harmonising-capacities.md).

## Spatial downscaling: region → country

!!! warning "Work in progress"
    The spatial downscaling is still work in progress. It currently does not match historical
    patterns.

This part is implemented in `iampypsa`. A model region like REMIND's `ECE` typically covers many
countries, whereas a PyPSA model requires country-level demand.

- **The region → country map** — `read_region_map()` reads the packaged region-mapping CSV
  (e.g. `data/remind/regions.csv` for REMIND) and returns `{region: [country, ...]}` (or the reverse, depending
  on `source`/`target`).
- **Proxy shares** — each sector's regional demand is weighted by *proxies*: normalised
  population and GDP projections (`downscale/proxy.py`) for most sectors, and degree-day-weighted
  demand proxies for heating (`heatpump`, `resistive`) and cooling (`space_cooling`). A sector's
  `sector_weights` entry names which proxies to blend and in what proportion (e.g.
  `{gdp: 0.6, population: 0.4}`).
- **Applying the split** — `disaggregate_demand_to_country()` applies those shares to every
  `(year, region, sector)` row. Most integrations don't call this directly; they call
  `Coupler.downscale_country_demand()`, which reads the regional demand and disaggregates it in
  one step.

## Temporal downscaling: annual → hourly

This part is **not implemented in `iampypsa`** — it happens on the PyPSA-model side, consistent
with the general split: IAM-side logic lives in the package, PyPSA-side logic lives in each
regional PyPSA model.

In each PyPSA model, the spatially downscaled total demand per (country, year, sector) then needs
to be downscaled temporally into an hourly timeseries by taking a demand shape from another
source and scaling it such that the profile's annual sum matches the country-level annual total.

## Next

- [Harmonising capacities](harmonising-capacities.md) — the equivalent reconciliation step for
  capacities, which does *not* go through this downscaling machinery.
- [Sector coupling](sector-coupling.md)
