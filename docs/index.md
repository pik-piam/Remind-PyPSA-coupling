# IAM-PyPSA Coupling

Welcome to the `iampypsa` docu! `iampypsa` is a package to support the soft-coupling of
Integrated Assessment Models (IAMs) and the [PyPSA](https://github.com/PyPSA) power system model
family, such as [PyPSA-Eur](https://pypsa-eur.readthedocs.io/en/latest/) or
[PyPSA-China](https://pik-piam.github.io/PyPSA-China-PIK/latest/). The package is written and
maintained by the Potsdam Institute for Climate Impact Research's
[Energy Transition Lab](https://www.pik-potsdam.de/en/institute/labs/energy-transition/energy-transition-lab).

!!! warning "Documentation under development"
    This documentation is preliminary and under active development.

The `iampypsa` package contains all logic that is identical for all PyPSA models that couple to
IAMs. These include:

- Reading IAM output
- Unit conversions
- Translation of demand, costs, capacities, CO2 prices
- Downscaling of demand from IAM region to countries

The interface to the PyPSA models' snakemake workflow is exposed via a small `Coupler` class.
Each snakemake workflow constructs one of its concrete subclasses (`RemindGdxCoupler` /
`RemindIamcCoupler` today) and calls its methods. Alternatively, functions can also be called
directly.

## Currently supported models

- **Integrated Assessment Models**
    - [REMIND](https://www.pik-potsdam.de/en/institute/departments/transformation-pathways/models/remind)
- **PyPSA models**
    - [PyPSA-Eur](https://pypsa-eur.readthedocs.io/en/latest/) (via [pypsa-eur-iam](https://github.com/pik-piam/pypsa-eur-iam))
    - [PyPSA-China](https://pik-piam.github.io/PyPSA-China-PIK/latest/)

New IAM models can make extensive use of the package but will require their own specific coupling
layer. See [Integrating a PyPSA model](getting-started/integrating-a-model.md).

Get in touch if you'd like to couple another IAM or PyPSA model.

## Start here

- **[Getting started](getting-started/index.md)**: A brief tutorial covering data exchange between IAMs and PyPSA and how to set up a new PyPSA model.
- **[Architecture](architecture.md)**: A deeper explanation of how the components fit together, what belongs in the package versus the model, the full `Coupler` interface, and the quantity/unit config.
- **[Reference](docs/reference/coupler.md)**: The auto-generated API reference for every module (also in the nav).

## What the package provides

| Operations | Module(s) |
|---|---|
| Open an IAM source and get the right coupler | `iampypsa.open_coupler` |
| Read IAM output (GDX / GAMS / IAMC) | `iampypsa.formats`, `iampypsa.loader.IamLoader` |
| Coupling names, mappings and units | `iampypsa.quantities`, `models/<iam>/quantities_*.yaml`, `iampypsa.units` |
| Transformations (CO₂, loads, capacities, costs, mapping) | `iampypsa.transforms` |
| Region → country downscaling | `iampypsa.downscale`, `iampypsa.reference.ssp` |
| Coupling interface | `iampypsa.Coupler`; per-IAM subclasses in `iampypsa.models` |

## Further reading

The methodology behind the REMIND–PyPSA coupling is described in full in:

> Odenweller, A. et al. (2026). REMIND-PyPSA-Eur: integrating power system flexibility into
> sector-coupled energy transition pathways. *Progress in Energy*.
> [https://doi.org/10.1088/2516-1083/ae3ffe](https://doi.org/10.1088/2516-1083/ae3ffe)

