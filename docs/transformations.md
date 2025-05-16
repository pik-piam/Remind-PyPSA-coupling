# Transformation Methods

## Techno-econmic data

For each key pair, a mapping method can be spefied:
- set_value: directly set the value to that specified in the "reference" column
- use_remind: use the remind tech specified in the "reference column"
- use_pypsa: use the pypsa name. The "reference" column has no effect
- use_remind_weighed_by:
- use_remind_with_learning_from: this is intended for technologies that are not implemented in REMIND. The base year cost will be set from pypsa and the cost will decrease in time as per the REMIND technology specified in the "reference" column. This method is only valid for "investment" costs

## Loads Techno-econmic data
