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