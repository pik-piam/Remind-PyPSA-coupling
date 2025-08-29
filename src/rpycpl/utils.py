""" Utility functions for the REMIND-PyPSA coupling"""

import os
import pandas as pd
import country_converter as coco
import functools
import logging

try:
    import gamspy
except ImportError:
    logging.warning("Gamspy not installed - GDX reading not available.")

READERS_REGISTRY = {}

# TODO write classes ro separate into files (readers/validators/etc)


def register_reader(name):
    """decorator factory to register ETL functions"""

    def decorator(func):
        READERS_REGISTRY[name] = func
        return func

    return decorator


# translate to pypsa
REMIND_NAME_MAP = {
    "ttot": "year",
    "tall": "year",
    "all_regi": "region",
    "all_te": "technology",
    "tePy32": "technology",
    "char": "parameter",
    "all_enty": "carrier",
}


def _fix_repeated_columns(cols) -> pd.DataFrame:
    found, result = [], []
    for i in range(len(cols)):
        if not cols[i] in found:
            result.append(cols[i])
        else:
            result.append(cols[i] + f"_{found.count(cols[i])}")
        found.append(cols[i])
    return result


def build_tech_map(remind2pypsa_map: pd.DataFrame, map_param="investment") -> pd.DataFrame:
    """
    Build a mapping from REMIND to PyPSA technology names using the mapping DataFrame.
    Adds groups in case mapping is not 1:1
    Args:
        remind2pypsa_map (pd.DataFrame): DataFrame with the (!!validated) mapping
        map_param (Optional, str):  the parameter to use for tech name mapping. 
            Defaults to 'investment'.
    Returns:
        pd.DataFrame: DataFrame with the mapping (remind_tech: PyPSA_tech, group)
    """

    if map_param not in remind2pypsa_map.parameter.unique():
        raise ValueError(
            f"Parameter {map_param} not found in the mapping file. "
            "Please check the mapping file and the parameter name."
        )
    tech_names_map = remind2pypsa_map.query(
        "mapper.str.contains('remind') & not mapper.str.contains('learn') & parameter == @map_param"
    )[["PyPSA_tech", "reference"]]
    tech_names_map.rename(columns={"reference": "remind_tech"}, inplace=True)
    tech_names_map.loc[:, "remind_tech"] = tech_names_map.remind_tech.apply(to_list)
    tech_names_map = tech_names_map.explode("remind_tech")
    tech_names_map.set_index("remind_tech", inplace=True)
    tech_names_map["group"] = tech_names_map.groupby(level=0).PyPSA_tech.apply(
        lambda x: " & ".join(x)
    )

    return tech_names_map


@register_reader("pypsa_costs")
def read_pypsa_costs(cost_files, **kwargs: dict) -> pd.DataFrame:
    """Read & stitch the pypsa costs files

    Args:
        cost_files (list): list of paths to the pypsa costs files
        **kwargs: additional arguments for pd.read_csv
    Returns:
        pd.DataFrame: the techno-economic data for all years.
    """
    pypsa_costs = pd.read_csv(cost_files.pop(), **kwargs)
    for f in cost_files:
        pypsa_costs = pd.concat([pypsa_costs, pd.read_csv(f)])
    return pypsa_costs


@register_reader("remind_csv")
def read_remind_csv(file_path: os.PathLike, **kwargs: dict) -> pd.DataFrame:
    """read an exported csv from remind (a single table of the gam db)

    Args:
        file_path (os.PathLike): path to the csv file
        **kwargs: additional arguments for pd.read_csv
    Returns:
        pd.DataFrame: the data.
    """
    df = pd.read_csv(file_path, **kwargs)
    # in case the parameter depended on the same set, all columns are suffixed with _1, _2, etc.
    df.columns = df.columns.str.replace(r"_\d$", "", regex=True)
    df.rename(columns=REMIND_NAME_MAP, inplace=True)

    df.columns = _fix_repeated_columns(df.columns)

    if "value" in df.columns:
        df.loc[:, "value"] = df.value.astype(float)

    return df


@register_reader("remind_regions")
def read_remind_regions_csv(mapping_path: os.PathLike, separator=",") -> pd.DataFrame:
    """read the export from remind

    Args:
        mapping_path (os.PathLike): the path to the remind mapping 
            (csv export of regi2iso set via GamsConnect)
        separator (str, optional): the separator in the csv. Defaults to ",".
    Returns:
        pd.DataFrame: the region mapping
    """
    regions = pd.read_csv(mapping_path)
    regions.drop(columns="element_text", inplace=True)
    regions["iso2"] = coco.convert(regions["iso"], to="ISO2")
    return regions


@register_reader("remind_descriptions")
def read_remind_descriptions_csv(file_path: os.PathLike) -> pd.DataFrame:
    """read the exported description from remind

    Args:
        file_path (os.PathLike): csv export from gamsconnect/embedded python
    Returns:
        pd.DataFrame: the descriptors per symbol, with units extracted
    """

    descriptors = pd.read_csv(file_path)
    descriptors["unit"] = descriptors["text"].str.extract(r"\[(.*?)\]")
    return descriptors.rename(columns={"Unnamed: 0": "symbol"}).fillna("")


@register_reader("remind_gdx")
def read_gdx(
    file_path: os.PathLike, variable_name: str, rename_columns={}, error_on_empty=True
) -> pd.DataFrame:
    """
    Auxiliary function for standardised and cached reading of REMIND-EU data
    files to pandas.DataFrame.

    Args:
        file_path (os.PathLike): Path to the GDX file.
        variable_name (str): Name of the symbol (param, var, scalar) to read from the GDX file.
        rename_columns (dict, optional): Dictionary for renaming columns. Defaults to {}.
        error_on_empty (bool, optional): Raise an error if the DataFrame is empty. Defaults to True.
    Returns:
        pd.DataFrame: the symbol table .
    """

    @functools.lru_cache
    def _read_and_cache_remind_file(fp):
        return gamspy.Container(load_from=fp)

    data = _read_and_cache_remind_file(file_path)[variable_name]

    df = data.records

    if error_on_empty and (df is None or df.empty):
        raise ValueError(f"{variable_name} is empty. In: {file_path}")

    df = df.rename(columns=rename_columns, errors="raise")
    df.metdata = data.description
    return df


def validate_file_list(file_list):
    """Validate the file list to ensure all files exist."""
    for file in file_list:
        if not os.path.isfile(file):
            raise FileNotFoundError(f"File {file} does not exist.")


def write_cost_data(cost_data: pd.DataFrame, output_dir: os.PathLike, descript: str = None):
    """Write the cost data to a folder, with one CSV file per year.

    Args:
        cost_data (pd.DataFrame): The cost data to write.
        output_dir (os.PathLike): The directory to write the file to.
        descript (str, optional): optioal description to add to the file name
    """

    if descript:
        output_dir += f"{descript}"
    for year, group in cost_data.groupby("year"):
        export_p = os.path.join(output_dir, f"costs_{year}.csv")
        group.to_csv(export_p, index=False)


def expand_years(df: pd.DataFrame, years: list) -> pd.DataFrame:
    """expand the dataframe by the years

    Args:
        df (pd.DataFrame): time-indep data
        years (list): the years

    Returns:
        pd.DataFrame: time-indep data with explicit years
    """

    return pd.concat([df.assign(year=yr) for yr in years])


def to_list(x: str) -> list | str:
    """in case of csv input. conver str to list

    Args:
        x (str): maybe list like string"""
    if isinstance(x, str) and x.startswith("[") and x.endswith("]"):
        split = x.replace("[", "").replace("]", "").split(", ")
        # in case no space in the text-list sep
        if split[0].find(",") >= 0:
            return x.replace("[", "").replace("]", "").split(",")
        else:
            return split
    return x


def key_sort(col):
    # if col.name == "year":
    #     return col.astype(int)
    if col.name == "technology":
        return col.str.lower()
    else:
        return col
