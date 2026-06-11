# Tools to couple REMIND & PySPA

## Overview
`rpycpl` is the **shared layer** of the REMIND ↔ PyPSA soft-coupling: it holds the logic
that is identical for every PyPSA model that couples to REMIND (reading REMIND output, unit
conversion, the cost/capacity/demand/CO₂ transforms, region→country downscaling) behind a
small adapter interface. Each PyPSA model supplies a thin **adapter** that subclasses
`rpycpl.CouplingAdapter` and is driven by that model's own Snakemake rules.

> REMIND-side logic lives in the package; PyPSA-side logic lives in the model. The adapter
> is the only seam where the two meet.

Test cases: PyPSA-EUR (Europe) and PyPSA-China-PIK (China); PyPSA-Earth is a near-term
target. See **[Architecture & usage](docs/architecture.md)** for how the components fit
together, what belongs in the package versus the adapter, and how to add a new model.

## quick start
1. install from pypi `pip install remind-pypsa-coupling`
2. import with `import rpycpl`

## Documentation
https://pik-piam.github.io/Remind-PyPSA-coupling/

# Installation (development)
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

# Usage
This package is intended for use in combination with REMIND and PyPSA, as part of a
snakemake workflow. In a coupled model you:

1. subclass `rpycpl.CouplingAdapter` in the model repo, implementing `build_config_overrides`
   and overriding only the hooks that genuinely differ for that model;
2. write thin Snakemake rules that build a `RemindLoader` + `load_symbol_specs()`, construct
   the adapter, call its methods, and write the outputs.

Worked examples (PyPSA-Eur and PyPSA-China adapters + rules) and a step-by-step walkthrough
are in **[docs/architecture.md](docs/architecture.md)**.

Activate the venv with `source .venv/bin/activate`



