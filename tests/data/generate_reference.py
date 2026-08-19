"""One-off generator for tests/data/reference/*.csv. Not run by pytest.

Snapshots known-correct output by running the real GDX coupler against the filtered fixtures in
this directory (see generate_fixtures.py, which must be run first). Future test runs diff their
own output against these CSVs, so any unintended behavior change shows up as a test failure.

Run once from the IAM-PyPSA-coupling repo root:
    python tests/data/generate_reference.py
"""

from pathlib import Path

import pandas as pd

from iampypsa.models.remind import RemindGdxCoupler
from iampypsa import IamLoader
    from iampypsa.models.remind import build_capacity_reporting_technologies
    from iampypsa.quantities import load_technology_parameters
from iampypsa.quantities import load_quantity_specs
from iampypsa.quantities.schema import get_iam_name
from iampypsa.transforms.costs import build_iam_techdata, convert_investment_to_input_capacity_basis

HERE = Path(__file__).parent
REF = HERE / "reference"

REGION_MAP = {"DEU": ["DE"], "EWN": ["AT", "BE", "LU", "NL"], "CHA": ["CN", "HK", "MO", "TW"]}
COUNTRIES = {"DE", "AT", "BE", "LU", "NL", "CN", "HK", "MO", "TW"}
SECTOR_WEIGHTS = {
    "AC": {"gdp": 0.6, "population": 0.4},
    "electrolysis": {"gdp": 0.7, "population": 0.3},
    "EV_pass": {"gdp": 0.3, "population": 0.7},
    "EV_freight": {"gdp": 0.5, "population": 0.5},
    "heatpump": {"gdp": 0.3, "population": 0.7},
}
YEARS = [2090, 2100]


def _coupler() -> RemindGdxCoupler:
    loader = IamLoader(str(HERE / "remind2pypsa_amt_filtered.gdx"))
    quantities = load_quantity_specs(backend=loader.backend)
    pop = pd.read_csv(HERE / "ssp_population_filtered.csv").set_index(["iso2", "year"])
    gdp = pd.read_csv(HERE / "ssp_gdp_filtered.csv").set_index(["iso2", "year"])
    return RemindGdxCoupler(
        loader,
        quantities,
        region_map=REGION_MAP,
        config={
            "sector_weights": SECTOR_WEIGHTS,
            "countries": COUNTRIES,
            "planning_horizons": YEARS,
        },
        model_regions=["DEU", "EWN", "CHA"],
        reference_data={"population": pop, "gdp": gdp},
    )


if __name__ == "__main__":
    REF.mkdir(exist_ok=True)
    coupler = _coupler()

    co2 = coupler.build_co2_prices(years=YEARS).rename(columns={"value": "co2_price"})
    co2.to_csv(REF / "co2_price.csv", index=False)
    print(f"co2_price.csv: {len(co2)} rows")

    demand = coupler.build_regional_demand()
    demand = demand[demand["year"].isin(YEARS)]
    demand.to_csv(REF / "sectoral_load.csv", index=False)
    print(f"sectoral_load.csv: {len(demand)} rows")

    country_demand = coupler.downscale_country_demand()
    country_demand.to_csv(REF / "sectoral_load_country.csv", index=False)
    print(f"sectoral_load_country.csv: {len(country_demand)} rows")

    tech_mapping = load_technology_parameters(str(HERE / "technology_mapping_example.yaml"))["technologies"]

    remind_long = coupler.extract_cost_parameters(2090)
    is_eff = (remind_long["parameter"] == "efficiency") & (remind_long["technology"] == "battery-inverter")
    remind_long.loc[is_eff, "value"] **= 2
    costs_raw = convert_investment_to_input_capacity_basis(
        build_iam_techdata(tech_mapping, remind_long), ["electrolysis"]
    )
    costs_raw.to_csv(REF / "costs_raw_overwritten.csv", index=False)
    print(f"costs_raw_overwritten.csv: {len(costs_raw)} rows")

    reports_capacity = build_capacity_reporting_technologies(load_quantity_specs(backend="iamc"))
    tmap = pd.DataFrame(
        [
            {"PyPSA": tech, "IAM": get_iam_name(tech, spec)}
            for tech, spec in tech_mapping.items()
            if get_iam_name(tech, spec) in reports_capacity
        ]
    )
    capacities = coupler.get_capacities(
        tmap, map_tech_col="IAM", map_carrier_col="PyPSA"
    )
    capacities.to_csv(REF / "installed_capacities.csv", index=False)
    print(f"installed_capacities.csv: {len(capacities)} rows")
