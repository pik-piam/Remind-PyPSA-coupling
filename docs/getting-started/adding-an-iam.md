# Adding a new IAM

Everything IAM-specific lives in one directory. Adding an IAM is four steps, and none of them
touch the generic layers.

!!! tip "Check first whether you need a coupler at all"
    If the IAM writes a standard IAMC exchange file, `model="iamc"` may already read it — and a
    file that deviates in a few variable names is served by an overlay YAML
    (`open_coupler(..., quantities_path="my_overlay.yaml")`), not by new code. Write a coupler
    only when the file needs *derived* quantities: residuals, unit bases that must be
    reconstructed, an efficiency chain.

## 1. Create the directory

```
src/iampypsa/models/<iam>/
  __init__.py
  coupler.py
  quantities_<backend>.yaml   # one per backend the IAM ships
  regions.csv                 # optional: the IAM's region → country map
```

## 2. Write the quantity specs

The YAML maps **coupling names** — `iampypsa`'s stable names (`co2_price`, `capacity`,
`demand_fe_sectors`, `cost_investment`, …) — onto the IAM's own symbol or variable names, plus
the unit each carries. Start by copying `models/iamc/quantities.yaml` and renaming the
variables. See [Data exchange](data-exchange.md) for the three spec shapes.

Every unit string must appear in `iampypsa.units.UNIT_CONVERSIONS`; an undeclared pair raises
rather than silently passing a wrong magnitude through.

## 3. Implement the two hooks

```python
from iampypsa.coupler import Coupler
from iampypsa.quantities.load import load_quantity


class MyIamCoupler(Coupler):
    """Coupler for <IAM> output."""

    def build_regional_demand(self):
        """Read regional sectoral demand as [year, region, sector, value, unit] in MWh."""
        return load_quantity(self.loader, self.quantities["demand_fe_sectors"])

    def extract_cost_parameters(self, year: int):
        """Extract [region, technology, parameter, value, unit] for one year."""
        ...
```

Those are the only two. `build_co2_prices`, `build_discount_rates`,
`downscale_country_demand`, `prepare_capacities` and `get_capacities` are inherited and
IAM-agnostic — if you find yourself overriding one, the divergence probably belongs in your
YAML or in the PyPSA model, not here.

Use `load_quantity` for every read: it dispatches on the spec's shape and raises if your
backend cannot serve it. `Coupler` is a plain class, not an ABC — deliberately.

## 4. Register it

One entry in `src/iampypsa/models/__init__.py`:

```python
MODELS["myiam"] = ModelSpec(
    package="iampypsa.models.myiam",
    quantity_configs={"iamc": "quantities_iamc.yaml"},
    couplers={"iamc": "iampypsa.models.myiam.coupler:MyIamCoupler"},
    region_map_reader="iampypsa.models.myiam.coupler:read_region_map",  # optional
)
```

The registry is pure data — classes are named as `"module:Class"` strings and imported on
demand — so entries never cycle back into the package. Add the packaged YAML/CSV to
`[tool.setuptools.package-data]` in `pyproject.toml`, then:

```python
coupler = open_coupler("myiam_output.mif", model="myiam", config=cfg)
```

## What must stay out

- **No IAM name outside `models/`.** `tests/test_imports.py` greps for this; the boundary
  erodes silently otherwise.
- **No PyPSA carrier vocabulary in the package.** `tech_map` stays an argument to
  `get_capacities` — carriers are the model's business.
- **No unit literals.** One row in `UNIT_CONVERSIONS`, applied once at the load seam.
