""" Example script of an ETL process using the rpycpl package with data loading from REMIND using csv
 (it's also possible to directly load from gdx)"""

import yaml
from typing import Any
import pandas as pd

from os import PathLike
import os.path

import rpycpl.utils as coupl_utils
from rpycpl.etl import ETL_REGISTRY, Transformation

# the config, typically loaded from a yaml file.
# the method is the name of the function in the ETL_REGISTRY
yaml_str = """
etl_steps:
  - name: loads
    method: "convert_load"
    frames:
      ac_load: p32_load
      h2_el_load: p32_h2elload
      heat_el_load: null
"""


def make_example_data():
    """mock remind export for testing"""
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

    ac_load.to_csv("p32_load.csv", index=False)
    h2_load.to_csv("p32_h2elload.csv", index=False)


class RemindLoader:
    """ A loader class that reads frames from exported csv files"""
    def __init__(self, remind_dir: PathLike):
        self.remind_dir = remind_dir

    def load_frames_csv(self, frames):
        paths = {
            k: os.path.join(self.remind_dir, v + ".csv")
            for k, v in frames.items()
            if v
        }
        return {k: coupl_utils.read_remind_csv(v) for k, v in paths.items()}


class ETLRunner:
    """Collection of methods to run ETL steps"""
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
    # make the fake data for the example
    make_example_data()

    # Parse the YAML string into a Python dictionary
    config = yaml.load(yaml_str, Loader=yaml.FullLoader)
    data_dir = os.path.abspath(".") # csvs were saved in the current directory

    region = "CHA"
    extra = "foo"
    # transform remind data
    steps = config.get("etl_steps", [])
    outputs = {}
    data_loader = RemindLoader(data_dir)
    for step_dict in steps:
        step = Transformation(**step_dict)
        frames = data_loader.load_frames_csv(step.frames)
        # example extra argument
        if step.method == "convert_load":
            outp = ETLRunner.run(step, frames, region=region)
        elif step.method == "my_method":
            # custom kwargs
            outp = ETLRunner.run(step, frames,  my_param=extra)
        else:
            outp = ETLRunner.run(step, frames)
        outputs[step.name] = outp

    # data in MWh
    print(outputs)