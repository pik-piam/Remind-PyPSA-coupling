"""One-off generator for tests/data/'s fixtures. Not run by pytest.

Filters the real GDX + mif from a REMIND run down to 2 years and 3 regions (one single-country,
two multi-country) and only the symbols/variables the package actually reads, so the test suite
in tests/ is self-contained (no external '/workspace/...' paths). Also generates the
tests/data/reference/*.csv snapshots by running the real couplers against the filtered fixtures.

Source data (see tests/data/README.md for full provenance): a REMIND run directory containing
    REMIND2PyPSA_AMT.gdx
    REMIND_generic_SSP2-EU21-PkBudg1000-AMT_withoutPlus.mif
and an SSP data directory containing population.csv/gdp.csv. Edit AMT_RUN/SSP_DIR below to point
at your local copies, then run once (needs a Python env with gamspy installed):
    python tests/data/generate_fixtures.py
"""

from pathlib import Path

import pandas as pd

YEARS = ["2090", "2100"]
# DEU: single-country; EWN: AUT+BEL+LUX+NLD; CHA: CHN+HKG+MAC+TWN (both multi-country)
REGIONS = ["DEU", "EWN", "CHA"]
COUNTRIES = ["DE", "AT", "BE", "LU", "NL", "CN", "HK", "MO", "TW"]

HERE = Path(__file__).parent
# Fill these in with your local REMIND run / SSP data directories before running this script.
AMT_RUN = Path("/path/to/remind/run/i1")
SOURCE_GDX = AMT_RUN / "REMIND2PyPSA_AMT.gdx"
SOURCE_MIF = AMT_RUN / "REMIND_generic_SSP2-EU21-PkBudg1000-AMT_withoutPlus.mif"
SSP_DIR = Path("/path/to/ssp/data")


# --- GDX -----------------------------------------------------------------------------------

#: symbol -> (gams_type, domain_names, year_col | None, region_col | None)
GDX_SYMBOLS = {
    "p32_load_sector":   ("Parameter", ["ttot", "all_regi", "loadPy32"], "ttot", "all_regi"),
    "p32_prodSeHydro":   ("Parameter", ["ttot", "all_regi"], "ttot", "all_regi"),
    "p_priceCO2":        ("Parameter", ["tall", "all_regi"], "tall", "all_regi"),
    "p_r":               ("Parameter", ["ttot", "all_regi"], "ttot", "all_regi"),
    "pe2se":             ("Set", ["all_enty_0", "all_enty_1", "all_te_2"], None, None),
    "pm_PEPrice":        ("Parameter", ["ttot", "all_regi", "all_enty"], "ttot", "all_regi"),
    "pm_data":           ("Parameter", ["all_regi", "char", "all_te"], None, "all_regi"),
    "pm_dataeta":        ("Parameter", ["tall", "all_regi", "all_te"], "tall", "all_regi"),
    "pm_emifac": (
        "Parameter",
        ["tall_0", "all_regi_1", "all_enty_2", "all_enty_3", "all_te_4", "all_enty_5"],
        "tall_0", "all_regi_1",
    ),
    "pm_eta_conv":       ("Parameter", ["tall", "all_regi", "all_te"], "tall", "all_regi"),
    "t":                 ("Set", ["ttot"], "ttot", None),
    "vm_cap":            ("Variable", ["tall", "all_regi", "all_te", "rlf"], "tall", "all_regi"),
    "vm_costTeCapital":  ("Variable", ["ttot", "all_regi", "all_te"], "ttot", "all_regi"),
}


def filter_gdx(source: Path, dest: Path) -> None:
    from gamspy import Container

    src = Container(load_from=str(source))
    out = Container()
    for name, (kind, domain, year_col, region_col) in GDX_SYMBOLS.items():
        records = src[name].records
        if year_col is not None:
            records = records[records[year_col].astype(str).isin(YEARS)]
        if region_col is not None:
            records = records[records[region_col].astype(str).isin(REGIONS)]
        records = records.reset_index(drop=True)
        print(f"  {name}: {len(records)} rows")
        if kind == "Parameter":
            out.addParameter(name, domain=domain, records=records)
        elif kind == "Variable":
            out.addVariable(name, domain=domain, records=records)
        else:
            out.addSet(name, domain=domain, records=records)
    out.write(str(dest))
    print(f"Wrote {dest}")


# --- mif -------------------------------------------------------------------------------------


def filter_mif(source: Path, dest: Path) -> None:
    from iampypsa.quantities import load_quantity_specs

    specs = load_quantity_specs(backend="iamc")
    variables: set[str] = set()
    for spec in specs.values():
        if not isinstance(spec, dict):
            continue
        if "variables" in spec:
            variables.update(spec["variables"])
        if "derived" in spec:
            for terms in spec["derived"].values():
                variables.update(v for _, v in terms)
        if "symbol" in spec and isinstance(spec["symbol"], str):
            variables.add(spec["symbol"])

    id_cols = ["Model", "Scenario", "Region", "Variable", "Unit"]
    raw = pd.read_csv(source, sep=";", dtype=str)
    raw = raw.loc[:, ~raw.columns.str.startswith("Unnamed")]
    year_cols = [c for c in raw.columns if c not in id_cols]
    keep_year_cols = [c for c in year_cols if c.strip() in YEARS]

    filtered = raw[raw["Variable"].isin(variables) & raw["Region"].isin(REGIONS)]
    filtered = filtered[id_cols + keep_year_cols]

    # read_iamc (pd.read_csv, no comment= support) needs a plain header row first -- provenance
    # lives in tests/data/README.md instead of an in-file comment.
    filtered.to_csv(dest, sep=";", index=False)
    print(f"Wrote {dest}: {len(filtered)} rows, {len(variables)} variables requested")


# --- SSP population/gdp -----------------------------------------------------------------------


def filter_ssp(source: Path, dest: Path) -> None:
    df = pd.read_csv(source)
    df = df[df["iso2"].isin(COUNTRIES) & df["year"].astype(str).isin(YEARS)]
    df.to_csv(dest, index=False)
    print(f"Wrote {dest}: {len(df)} rows")


# --- region mapping ----------------------------------------------------------------------------


def filter_region_mapping(dest: Path) -> None:
    source = HERE.parents[1] / "src" / "iampypsa" / "data" / "remind" / "regions.csv"
    df = pd.read_csv(source, sep=";")
    df = df[df["RegionCode"].isin(REGIONS)]
    df.to_csv(dest, sep=";", index=False)
    print(f"Wrote {dest}: {len(df)} rows")


# --- synthetic degree-day fixtures --------------------------------------------------------------


def write_synthetic_degree_days(cdd_dest: Path, hdd_dest: Path) -> None:
    """Small synthetic CDD/HDD fixtures matching io.degree_days.read_degree_days's schema.

    Not from the AMT run -- degree-days are a separate climate dataset with no real source in
    this workspace. Schema-correct, plausible values for DE/AT/BE/LU/NL at 2060 (the nearest
    decade the real dataset ships; degree-day tests don't depend on matching YEARS above).
    """
    # Schema per io/degree_days.py: [year, country(ISO3), type, tlim_setpoint, rcp, ssp, value].
    iso3s = ["DEU", "AUT", "BEL", "LUX", "NLD"]
    rows_cdd = [
        {"year": 2060, "country": iso3, "type": "CDD", "tlim_setpoint": 22, "rcp": "4_5", "ssp": "SSP2", "value": v}
        for iso3, v in zip(iso3s, [45.2, 12.1, 30.4, 28.9, 33.7])
    ]
    rows_hdd = [
        {"year": 2060, "country": iso3, "type": "HDD", "tlim_setpoint": 15, "rcp": "4_5", "ssp": "SSP2", "value": v}
        for iso3, v in zip(iso3s, [2150.0, 2890.5, 2201.3, 2350.7, 2100.9])
    ]
    pd.DataFrame(rows_cdd).to_csv(cdd_dest, index=False)
    pd.DataFrame(rows_hdd).to_csv(hdd_dest, index=False)
    print(f"Wrote {cdd_dest}, {hdd_dest}")


if __name__ == "__main__":
    HERE.mkdir(exist_ok=True)

    print("Filtering GDX...")
    filter_gdx(SOURCE_GDX, HERE / "remind2pypsa_amt_filtered.gdx")

    print("Filtering mif...")
    filter_mif(SOURCE_MIF, HERE / "remind_generic_amt_filtered.mif")

    print("Filtering SSP population/gdp...")
    filter_ssp(SSP_DIR / "population.csv", HERE / "ssp_population_filtered.csv")
    filter_ssp(SSP_DIR / "gdp.csv", HERE / "ssp_gdp_filtered.csv")

    print("Filtering region mapping...")
    filter_region_mapping(HERE / "region_mapping_filtered.csv")

    print("Writing synthetic degree-day fixtures...")
    write_synthetic_degree_days(HERE / "cdd_filtered.csv", HERE / "hdd_filtered.csv")

    print("Done. Run generate_reference.py next to snapshot expected outputs.")
