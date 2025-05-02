""" Utility functions for the REMIND-PyPSA coupling"""

import os
import pandas as pd
import country_converter as coco
import functools

# import gamspy

# translate to pypsa
REMIND_NAME_MAP = {
    "ttot": "year",
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


def read_remind_csv(file_path: os.PathLike, **kwargs) -> pd.DataFrame:
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


def read_remind_regions_csv(mapping_path: os.PathLike, separator=",") -> pd.DataFrame:
    """read the export from remind

    Args:
        mapping_path (os.PathLike): the path to the remind mapping (csv export of regi2iso set via GamsConnect)

    Returns:
        pd.DataFrame: the region mapping
    """
    regions = pd.read_csv(mapping_path)
    regions.drop(columns="element_text", inplace=True)
    regions["iso2"] = coco.convert(regions["iso"], to="ISO2")
    return regions


def read_remind_descriptions_csv(file_path: os.PathLike) -> pd.DataFrame:
    """read the exported description from remind
    Args:
        file_path (os.PathLike): csv export from gamsconnect/embedded python
    Returns:
        pd.DataFrame: the descriptors per symbol, with units extracted
    """

    descriptors = pd.read_csv("/home/ivanra/documents/gams_learning/pypsa_export/descriptions.csv")
    descriptors["unit"] = descriptors["text"].str.extract(r"\[(.*?)\]")
    return descriptors.rename(columns={"Unnamed: 0": "symbol"}).fillna("")


def read_gdx(file_path: os.PathLike, variable_name: str, rename_columns={}, error_on_empty=True):
    """
    Auxiliary function for standardised and cached reading of REMIND-EU data
    files to pandas.DataFrame.

    Here all values read are considered variable, i.e. use
    "variable_name" also for what is considered a "parameter" in the GDX
    file.
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
