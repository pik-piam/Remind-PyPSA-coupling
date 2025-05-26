""" Basic example script of an ETL process using the rpycpl package."""

import yaml
from typing import Any
import pandas as pd

from rpycpl.etl import ETL_REGISTRY, Transformation

# the config, typically loaded from a yaml file.
# the method is the name of the function in the ETL_REGISTRY, the fields are defined
# by the transformation dataclass
yaml_str = """
etl_steps:
  - name: loads
    method: "convert_load"
    frames:
      ac_load: p32_load
      h2_el_load: null
      heat_el_load: null
"""


# this could be function. The class is explicit and allows for more complex workflows later if needed
class ETLRunner:

    @staticmethod
    def run(step: Transformation, frames: dict[str, pd.DataFrame], **kwargs) -> pd.DataFrame:
        """Run the ETL step with the given frames and parameters.
        Args:
            step (Transformation): The ETL step to run.
            frames (dict): Dictionary of dataframes with loads
            kwargs: Additional keyword arguments for the ETL method.
        Returns:
            pd.DataFrame: The transformed dataframe.
        """
        method = step.name if not step.method else step.method
 
        func = ETL_REGISTRY.get(method)
        if not func:
            raise ValueError(f"ETL method '{method}' not found in registry.")
        if kwargs:
            kwargs.update(step.kwargs)
            return func(frames, **kwargs)
        else:
            return func(frames)


if __name__ == "__main__":

    # Parse the YAML string into a Python dictionary
    config = yaml.load(yaml_str, Loader=yaml.FullLoader)
    region = "CHA"

    # Example Remind Symbol Data in TW/yr (typically loaded from a CSV or GDX)
    ac_load = pd.DataFrame({
        'year': [2030, 2035, 2040, 2045, 2050],
        'region': ['CHA'] * 5,
        'value': [1.396324, 1.450305, 1.488186, 1.602782, 1.650218],
    })
    h2_load = pd.DataFrame({
        'year': [2030, 2035, 2040, 2045, 2050],
        'region': ['CHA'] * 5,
        'value': [0 ,0, 0, 0,0],
    })
    frames = {
        'ac_load': ac_load,
        'h2_el_load': h2_load,
    }

    allowed = Transformation.__dict__["__annotations__"]
    print(f"allowed yaml keys for etl_step:\n{allowed}")

    # illustrate all methods available by default
    print("Available ETL methods:")
    for name in ETL_REGISTRY.keys():
        print(f" - {name}")

    # ==== Actual code: transform remind data ====
    outputs = {}
    steps = config.get("etl_steps", [])
    for step_dict in steps:
        step = Transformation(**step_dict)
        # convert load takes an extra arg
        if step.method == "convert_load":
            outp = ETLRunner.run(step, frames, region=region)
        else:
            outp = ETLRunner.run(step, frames)

        outputs[step.name] = outp

    # data in MWh
    print(outputs)