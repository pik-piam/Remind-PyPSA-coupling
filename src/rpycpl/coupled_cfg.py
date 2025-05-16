"""
Tools / Script to generate a PyPSA config file for REMIND-PyPSA coupling.

Usage:\n
    python coupled_cfg.py\n
    cd pypsa_folder\n
    snakemake --configfile=remind.yaml\n

Missing:\n
- arguments or config
"""

import os
import yaml
from .utils import read_remind_csv

if __name__ == "__main__":

    region = "CHA"
    # base_p = "/home/ivanra/documents/gams_learning/pypsa_export"
    base_p = os.path.expanduser(
        "~/downloads/output_REMIND/SSP2-Budg1000-PyPSAxprt_2025-05-09/pypsa_export"
    )

    # load relevant data
    co2_p = (
        read_remind_csv(os.path.join(base_p, "p_priceCO2.csv"))
        .query("region == @region")
        .drop(columns=["region"])
        .set_index("year")
    )

    # get remind version
    with open(os.path.join(base_p, "c_model_version.csv"), "r") as f:
        remind_v = f.read().split("\n")[1].replace(",", "").replace(" ", "")

    with open(os.path.join(base_p, "c_expname.csv"), "r") as f:
        remind_exp_name = f.read().split("\n")[1].replace(",", "").replace(" ", "")

    # read template config
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    with open(os.path.join(root_dir, "data/eg_remind.yaml")) as f:
        template_cfg = yaml.safe_load(f)

    cfg = template_cfg.copy()
    cfg["scenario"]["planning_horizons"] = co2_p.index.tolist()
    # TODO get run mame & v"
    # TODO add costs path
    sc_name = "remind_ssp2NPI"
    cfg["co2_scenarios"][sc_name]["pathway"] = co2_p["value"].to_dict()
    cfg["scenario"]["co2_pathway"] = [sc_name]

    remind_cfg = {
        "remind": {
            "coupling": "1way",
            "version": remind_v,
            "run_name": remind_exp_name,
        }
    }
    cfg["run"].update({"is_remind_coupled": True})
    cfg.update(remind_cfg)

    with open(os.path.join(root_dir, "data/eg_remind_output.yaml"), "w") as f:
        yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)

    # TODO read  remind regions and write to config
    # TODO centralise joint settings = overwrite hours
    # TODO add disagg config
