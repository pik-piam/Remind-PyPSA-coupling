# Tools to couple REMIND & PySPA

## Overview
This is a collection of tools to couple remind and pypsa for different regions.

The tools are currently in development, with as test cases PyPSA-EUR for Germany and PyPSA-China-PIK for China.

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
This package is intended for use in combination with REMIND and PyPSA, as part of a snakemake workflow

Examples: Coming at some point

Activate the venv with `source .venv/bin/activate`



