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

# Usage
This package is intended for use in combination with REMIND and PyPSA, as part of a snakemake workflow

Examples: Coming at some point

# Mappings and ETL operations

### Technology mapping
The technology mapping is controlled by the a tech mapping file. Only (PyPSA_technology, parameter) key pairs specified in the mapping will be written out.

For each key pair, a mapping method can be spefied:
- set_value: directly set the value to that specified in the "reference" column
- use_remind: use the remind tech specified in the "reference column"
- use_pypsa: use the pypsa name. The "reference" column has no effect
- use_remind_weighed_by:
- use_remind_with_learning_from: this is intended for technologies that are not implemented in REMIND. The base year cost will be set from pypsa and the cost will decrease in time as per the REMIND technology specified in the "reference" column. This method is only valid for "investment" costs

Validation will be applied to the mappings file and input costs.

### Regional disaggregation


### Country to region disaggregation


### Temporal transformations


