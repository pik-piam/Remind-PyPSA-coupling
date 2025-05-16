# Data and objects
This page introduces the objects and data expected by the coupling layer
## Data

### Mapping Table
The REMIND-PyPSA mapping allows to build the the techno-economic data from the pypsa and remind techno-economic data 

| Variable | Description | Allowed values | example |
|--------------------|---------------|------------|-----------|
| PyPSA_tech | pypsa tech names | as expected by network creation | `OCGT` |
| parameter  | the techno-economic field | `[CO2 itensity, efficiency, FOM, fuel, investment, lifetime, VOM] ` | - |
| mapper    | func to make the pypsa parameter | `["set_value", "use_remind", use_pypsa", "weigh_remind_by_gen"]`| - |
| reference | the value to pass to the mapper | `set_value: 1.2, use_pysa:(ignored), use_remind: biochp (remind_name), weigh_remind_by_gen: "[biochp, bioigcc]"` |
| unit     | the reference unit. Note pypsa load cost sometimes uses this! | str, missing | USD/Mwh, % (for FOM) |  
| comment | additional comments that will be added to pypsa_cost csv entry | str | "Dummy value to avoid issues" |

Example:

PyPSA_tech|parameter|mapper|reference|unit|comment|
|----|-----|----|----|----|----|
battery inverter|CO2 intensity|set_value|0|tCO2/MWh_th| |
biomass|investment|weigh_remind_by_gen|"[biochp, bioigcc, bioigccc]"| |

The pypsa <-> REMIND tech name map is derived from this table (using the investment parameter by default).

### REMIND Data

The following data are needed from REMIND
- techno-economic parameters `pm_data`
- weights for technology baskets: e.g `p32_weightGen`,
- CO2 prices `p_priceCO2`
- pre-investment capacities (several options)
- AC load `p32_load`
- capex `p32_capCost` (several options),
- eta `pm_dataeta`, `pm_eta_conv`,
- fuel_costs `p32_PEPriceAvg`,
- discount_r `p32_discountRate`,
- co2_intensity `pm_emifac`,
- run name `c_expname`
- version `c_modelversion`

These can either be exported in the gdx format or a series of csvs.
Implicit in these are the years and regions.

### PyPSA data
- the cost data: in case the pypsa-cost data ends before the remind horizon, the missing values will be fixed to the last available time point
- the existing infrastructure data ([powerplantmatching](https://powerplantmatching.readthedocs.io/en/latest/) or equiv)

## Objects

Each of these data are represented as pandas `pd.Dataframe` or `pd.Series`.
Whilst there is validation - in particular of the mapping - the package expects column names as per powerplantmatching, pypsa and REMIND (GAMS sets in this case).

The various routines also merge pypsa and remind data. Some processing functions will only worked with the merged data. The merged data should have `suffixes=("_remind", "_pypsa")`.



