# Tools to couple REMIND & PySPA

## Overview
This is a collection of tools to couple remind and pypsa for different regions.

The tools are currently in development, with as test cases PyPSA-EUR for Germany and PyPSA-China-PIK for China.


### Coupling steps
1. Export data from within REMIND (REMIND - pypsa forks)
2. ETL (extract transform, load) & save data for PyPSA workflow execution according to config. (This repo)
3. Prepare pypsa execution config as needed (this repo)
4. Run PyPSA workflow (dedicated repos or pypsa-earth)
5. ETL for REMIND (This repo_)

# Installation (local editable copy)
We recommend using `uv`. 
1. install uv
2. in the project workspace run `uv pip install -e .`
3. make a venv `uv venv` at `project/.venv`
4. sync venv with requirements `uv sync`

# Package description

### Technology mapping


### Regional disaggregation


### Country to region disaggregation


### Temporal transformations


