# IAM-PyPSA-coupling: Coupling Integrated Assessment Models with PyPSA

This repository contains tools to couple long-term Integrated Assessment Models (IAMs) with high-resolution Energy System Models (ESMs) based on [PyPSA](https://github.com/pypsa/pypsa). Coupling IAMs with ESMs can combine the complementary strengths of both model types, jointly optimising long-term transformation pathways and short-term power system operation.

IAM-PyPSA-coupling currently focuses on unidirectional coupling from the IAM [REMIND](https://github.com/remindmodel/remind) to PyPSA, specifically [PyPSA-Eur](https://github.com/pik-piam/pypsa-eur-iam) and [PyPSA-China](github.com/pik-piam/PyPSA-China-PIK). Data can be read from REMIND's native GAMS GDX output or from the standardised [IAMC format](https://docs.ece.iiasa.ac.at/iamc.html).

> [!NOTE]
> This package is under active development, led by the Potsdam Institute for Climate Impact Research's [Energy Transition Lab](https://www.pik-potsdam.de/en/institute/labs/energy-transition).

## Overview

IAM-PyPSA-coupling (`iampypsa`) contains all logic that is identical for every PyPSA model such as reading IAM output, unit conversions, transformations of demand/costs/capacity, and downscaling of sectoral demand from IAM regions to country level.

## Supported IAMs

Currently supported IAMs include:

- [REMIND](https://github.com/remindmodel/remind) (under development)
- Interested in adding your IAM? Contact us!

## Supported PyPSA models

PyPSA models call the package from their snakemake workflow, which needs to be adapted accordingly.

Currently supported PyPSA models (under active development) include:

* PyPSA-Eur, see dedicated [PyPSA-Eur-IAM](https://github.com/pik-piam/pypsa-eur-iam) repository
* PyPSA-China, see dedicated [PyPSA-China-PIK](github.com/pik-piam/PyPSA-China-PIK) repository

## Installation (development)

To be updated.

We recommend using `uv`. 
1. install uv
2. make a venv `uv venv` at `project/.venv`
3. Activate the venv with `source .venv/bin/activate`
4. option a) In the project folder run `uv pip install -e .` Then use as a package
4. option b) In the project workspace update the venv with `uv sync` to have all the package requirements. You can then use the src files as standalone.

> [!NOTE]
> `uv` sometimes causes issues at steps 4. In this case 
> - run `uv pip install pip` after step 3
> - run `pip install -e .` in the project worspace

## Documentation

https://pik-piam.github.io/IAM-PyPSA-coupling/ (to be updated)

TODO: Update documentation with concrete steps how to use coupling package, data types exchanged between models, examples.

## Usage

To be updated.

## Further information

For more background on the model coupling of IAMs and ESMs see:

> Odenweller, A. et al. (2026). REMIND-PyPSA-Eur: integrating power system flexibility into sector-coupled energy transition pathways. *Progress in Energy*. https://doi.org/10.1088/2516-1083/ae3ffe