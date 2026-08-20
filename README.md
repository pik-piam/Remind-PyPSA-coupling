# IAM-PyPSA-coupling: Coupling Integrated Assessment Models with PyPSA

This repository contains tools to couple long-term Integrated Assessment Models (IAMs) with high-resolution Energy System Models (ESMs) based on [PyPSA](https://github.com/pypsa/pypsa). Coupling IAMs with ESMs can combine the complementary strengths of both model types, jointly optimising long-term transformation pathways and short-term power system operation.

IAM-PyPSA-coupling currently focuses on unidirectional coupling from the IAM [REMIND](https://github.com/remindmodel/remind) to PyPSA, specifically [PyPSA-Eur](https://github.com/pik-piam/pypsa-eur-iam) and [PyPSA-China](https://github.com/pik-piam/PyPSA-China-PIK). Currently, data can be read from REMIND's native GAMS GDX output or from the standardised [IAMC format](https://docs.ece.iiasa.ac.at/iamc.html).

> [!NOTE]
> This package is under active development, led by the Potsdam Institute for Climate Impact Research's [Energy Transition Lab](https://www.pik-potsdam.de/en/institute/labs/energy-transition).

## Overview

IAM-PyPSA-coupling (`iampypsa`) contains all logic that is identical for every PyPSA model such as reading IAM output, unit conversions, transformations of demand/costs/capacity, and downscaling of sectoral demand from IAM regions to country level.

## Supported IAMs

Currently supported IAMs include:

- [REMIND](https://github.com/remindmodel/remind) (under active development)

Interested in adding your IAM? Contact us!

## Supported PyPSA models

Currently supported PyPSA models (under active development) include:

- PyPSA-Eur, see dedicated [PyPSA-Eur-IAM](https://github.com/pik-piam/pypsa-eur-iam) repository
- PyPSA-China, see dedicated [PyPSA-China-PIK](https://github.com/pik-piam/PyPSA-China-PIK) repository

PyPSA models call the package from their snakemake workflow, which needs to be adapted accordingly (see [Getting started](https://pik-piam.github.io/IAM-PyPSA-coupling/getting-started/))

## Installation (development)

We recommend using `uv`.
1. install uv
2. make a venv `uv venv` at `project/.venv`
3. Activate the venv with `source .venv/bin/activate`
4. option a) In the project folder run `uv pip install -e .` Then use as a package
4. option b) In the project workspace update the venv with `uv sync` to have all the package requirements. You can then use the src files as standalone.

> [!NOTE]
> `uv` sometimes causes issues at steps 4. In this case
> - run `uv pip install pip` after step 3
> - run `pip install -e .` in the project workspace

Optional extras: `gdx` (read REMIND `.gdx` output via `gamspy`), `ssp` (fetch SSP proxy data
live from the IIASA API), `docu` (build these docs locally), `jupyter`. E.g.
`uv pip install -e ".[gdx,ssp]"`.

## Usage

`build_coupler` detects the source format, pairs it with the matching coupler for that IAM, and returns it ready to use from PyPSA's Snakemake rules:

```python
from iampypsa import build_coupler

coupler = build_coupler(
    "REMIND.mif",          # or REMIND.gdx — the suffix selects the backend and the coupler
    model="remind",
    config={
        "planning_horizons": [2030, 2050],
        "sector_weights": {"AC": {"gdp": 0.6, "population": 0.4}},
        "countries": {"DE", "FR", "PL"},
    },
    reference_data={"population": population_df, "gdp": gdp_df},
)

demand = coupler.downscale_country_demand()    # country-level annual sectoral demand
co2_prices = coupler.build_co2_prices()        # regional CO2 price pathway
costs = coupler.extract_cost_parameters(2030)  # cost components for one year
```

For a single step, you may also reach past the facade and call the layers directly:

```python
from iampypsa import IamLoader, load_quantity_specs
from iampypsa.quantities import load_quantity
from iampypsa.transforms.co2_prices import extract_co2_prices
from iampypsa.transforms.costs import apply_currency_factor

loader = IamLoader("REMIND.mif")
quantities = load_quantity_specs(backend=loader.backend)

raw = load_quantity(loader, quantities["co2_price"])  # unit conversion applied here
prices = extract_co2_prices(raw, regions=["DEU", "FRA"], years=[2030, 2050])
co2_prices = apply_currency_factor(prices, currency_factor=1.0, parameters=None)
```

See [Getting started](https://pik-piam.github.io/IAM-PyPSA-coupling/getting-started/) for a more complete tutorial.

> [!IMPORTANT]
> Upgrading a consumer from before the 2026-08 restructure (`iampypsa.io`, `RemindLoader`,
> `load_symbol_specs`)? There are no shims — see [`docs/migration.md`](docs/migration.md).

## Documentation

https://pik-piam.github.io/IAM-PyPSA-coupling/ — start at
[Getting started](https://pik-piam.github.io/IAM-PyPSA-coupling/getting-started/) for a tutorial walkthrough or [Architecture](https://pik-piam.github.io/IAM-PyPSA-coupling/architecture/) for the design rationale.

## Further information

For more background on the model coupling of IAMs and ESMs see:

> Odenweller, A. et al. (2026). REMIND-PyPSA-Eur: integrating power system flexibility into sector-coupled energy transition pathways. *Progress in Energy*. https://doi.org/10.1088/2516-1083/ae3ffe