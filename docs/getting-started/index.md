# Getting started

These pages explain what `iampypsa` does and how to use it.

1. **[Integrating a PyPSA model](integrating-a-model.md)** — the general approach for integrating
   `iampypsa` into PyPSA's snakemake workflow.
2. **[Data exchange](data-exchange.md)** — what data can be exchanged between IAMs and PyPSA.
3. **[Technology mapping](technology-mapping.md)** — the YAML file that decides, per technology
   and parameter, whether data comes from the IAM or PyPSA or is manually set.
4. **[Downscaling demand](downscaling-demand.md)** — downscaling of annual sectoral demand to
   country-level demand, necessary if IAM regions are coarser than country-level.
5. **[Harmonising capacities](harmonising-capacities.md)** — reconciling the IAM's capacity
   targets with PyPSA's existing power plant database.
6. **[Sector coupling](sector-coupling.md)** — the default approach used for sector-specific
   electricity demand (electrolysis, EVs, heat).
