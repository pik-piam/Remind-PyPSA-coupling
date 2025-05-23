# Examples

## abstracted logic
You can find simple examples of using the abstracted methods in the examples folder. These have trivial transformations

These include:

- `basic_script.py`: a basic abstracted ETL logic based on a `yaml` description of the steps, with fake local data. 
- `read_and_transform_remind.py`: as above but using a data loader. The example loads fake csv data  generated as part of the example.
- `custom_transformations.py` an example of how to register custom transformations 


The examples are unfortunately untested.

## Direct scripts
if you do not want to use abstracted logic, a one-off script example 

- `one-offscript.py`: a script using the csv reader

## Exporting REMIND data
There are three possibilities to export data from GAMS:

1. make your own GDX and export only the needed symbols (params, sets, ..). You may want to make sub-sets and new symbols to control the export. 
2. Use the `embeddedCode` and `gamsconnect` functionalities to export to csv. This has the advantage that the data is immediately readable to external users.
3. Use the `fulldata.gdx` - not recommended as too implicit.

Example for option 2:
```
EmbeddedCode Python:
"""
Programatic loop over gams connect CSV export for all export PARAMS
"""
import yaml, os
from gams.connect import ConnectDatabase


PARAMS = ["part1", "set1"]


dir = "./pypsa_export"
if not os.path.isdir(dir):
    os.mkdir(dir)

# single gams connect yaml nstruction
export_instruct = '''
    - GAMSReader:
        symbols:
          - name: {par}
    - CSVWriter:
        file: {dir}/{par}.csv
        name: {par}
        valueSubstitutions: {'EPS': 0}
    '''

# Loop
cdb = ConnectDatabase(gams._system_directory, ecdb=gams)
for par in PARAMS:
    par_instr = yaml.safe_load(export_instruct.replace("{par}",par).replace("{dir}",dir))
    try:
        # the gams connect export
        cdb.execute(par_instr)
    except Exception as e:
        gams.printLog(f"Error par {par} skipped: {e}")

endEmbeddedCode
```

# Integration with snakemake

The idea is to use a snakemakre rule's `snakemake.params`, `snakemake.inputs` and `snakemake.outputs` + a section of your config (loaded from yaml) to control execution.

1. the config has the ETL transformation steps. The config can be added to the snakefile or passed via the CLI using `--configfile=<myfile>` 
2. the data locations are from `snakemake.inputs` (idem for outputs)

We recommend splitting the ETL and disagg step into two rules. If you want a more explicit representation in the workflow, you can use the same one or two script with many rules and different arguments.

