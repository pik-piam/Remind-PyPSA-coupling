# Getting started

These pages walk through what `iampypsa` does and how to use it, in the order that makes sense
if you're integrating a new PyPSA model or just want to understand the coupling:

1. **[Data Exchange](data-exchange.md)** — what data is exchanged between IAMs and PyPSA:
   demand, costs, capacities, CO2 prices.
2. **[How the Package Works](integrating-a-model.md)** — the general approach for integrating
   `iampypsa` into PyPSA's snakemake workflow.
3. **[Technology Mapping](technology-mapping.md)** — the YAML file that decides, per technology
   and parameter, whether data comes from the IAM or the PyPSA model default.
4. **[Downscaling Demand](downscaling-demand.md)** — downscaling of annual sectoral demand to
   country-level demand.
5. **[Harmonising Capacities](harmonising-capacities.md)** — reconciling the IAM's capacity
   targets with PyPSA's existing power plant database.
6. **[Sector Coupling](sector-coupling.md)** — the stylised approach used for sector-specific
   electricity demand (electrolysis, EVs, heat).
