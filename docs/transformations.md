# Transformation Methods

## Techno-economic data

This builds the coupled techno-economic data for the PyPSA model from REMIND parameters. Alternatively, values can be directly set or taken from PyPSA. For each target parameter (key pair), a [mapping method](../objects#mapping-table) can be specified (in order of preference):

- **use_remind**: use the remind tech specified in the "reference column"
- **use_remind_weighed_by**: aggregate different remind technologies together using a weight (eg generation share)
- **set_value**: directly set the value to that specified in the "reference" column
- **use_pypsa**: use the pypsa name. The "reference" column has no effect
- **use_remind_with_learning_from**: this is intended for technologies that are not implemented in REMIND. The base year cost will be set from pypsa and the cost will decrease in time as per the REMIND technology specified in the "reference" column. This method is only valid for "investment" costs

## Loads data
- conversion to MWh

## Spatial Disaggregation
- pre-defined reference data
