"""Tests for read_degree_days and CDD/HDD proxy weighting via the named-proxy registry."""

from pathlib import Path

import pandas as pd
import pytest

from iampypsa.downscale import (
    build_demand_proxy_from_dd,
    build_proxy_shares,
    disaggregate_demand_to_country,
    normalise,
)
from iampypsa.io import read_degree_days

CDD = Path(__file__).parent / "data" / "cdd_filtered.csv"
HDD = Path(__file__).parent / "data" / "hdd_filtered.csv"


def _proxy(vals: dict[str, float], year: int = 2060) -> pd.DataFrame:
    """A MultiIndex[(iso2, year)] value frame, the proxy contract shape."""
    return pd.DataFrame(
        {"iso2": list(vals), "year": [year] * len(vals), "value": list(vals.values())}
    ).set_index(["iso2", "year"])


# -- build_proxy_shares: replace-for-heat/cool via {heating_demand:1.0}/{cooling_demand:1.0} --


def test_build_proxy_shares_heating_demand_only_ignores_pop_gdp():
    proxies = {
        "population": _proxy({"CN": 1000.0, "TW": 1.0}),  # skewed to CN
        "gdp": _proxy({"CN": 1000.0, "TW": 1.0}),         # skewed to CN
        "heating_demand": _proxy({"CN": 3.0, "TW": 1.0}),  # 3:1
    }
    shares = build_proxy_shares(
        ["CN", "TW"], 2060, "resistive", proxies, {"resistive": {"heating_demand": 1.0}}
    )
    assert sum(shares.values()) == pytest.approx(1.0)
    # Pure heating-demand split — pop/gdp skew is ignored.
    assert shares["CN"] == pytest.approx(0.75)
    assert shares["TW"] == pytest.approx(0.25)


def test_build_proxy_shares_cooling_demand_only():
    proxies = {
        "population": _proxy({"CN": 5.0, "TW": 5.0}),
        "gdp": _proxy({"CN": 5.0, "TW": 5.0}),
        "cooling_demand": _proxy({"CN": 1.0, "TW": 4.0}),
    }
    shares = build_proxy_shares(
        ["CN", "TW"], 2060, "space_cooling", proxies, {"space_cooling": {"cooling_demand": 1.0}}
    )
    assert shares["TW"] == pytest.approx(0.8)
    assert shares["CN"] == pytest.approx(0.2)


def test_build_proxy_shares_ac_gdp_pop_blend():
    pop = _proxy({"DE": 80.0, "FR": 60.0}, year=2030)
    gdp = _proxy({"DE": 40.0, "FR": 10.0}, year=2030)
    weights = {"AC": {"gdp": 0.5, "population": 0.5}}
    shares = build_proxy_shares(["DE", "FR"], 2030, "AC", {"population": pop, "gdp": gdp}, weights)
    assert shares == pytest.approx({"DE": 0.6857142857142857, "FR": 0.3142857142857143})


def test_build_proxy_shares_missing_proxy_raises():
    with pytest.raises(ValueError, match="heating_demand"):
        build_proxy_shares(
            ["CN", "TW"], 2060, "resistive",
            {"population": _proxy({"CN": 1.0, "TW": 1.0})},
            {"resistive": {"heating_demand": 1.0}},
        )


def test_build_proxy_shares_year_clamp():
    # heating_demand frame only has 2060; requesting 2045 must clamp up to 2060 (no error).
    proxies = {"heating_demand": _proxy({"CN": 3.0, "TW": 1.0}, year=2060)}
    shares = build_proxy_shares(
        ["CN", "TW"], 2045, "resistive", proxies, {"resistive": {"heating_demand": 1.0}}
    )
    assert shares["CN"] == pytest.approx(0.75)


def test_disaggregate_heating_by_heating_demand_ac_by_gdp_pop():
    """One call: heating splits by heating-demand proxy, AC by GDP/pop, in the same region."""
    load = pd.DataFrame({
        "year": [2060, 2060], "region": ["EUR", "EUR"],
        "sector": ["resistive", "AC"], "value": [100.0, 100.0], "unit": ["MWh", "MWh"],
    })
    proxies = {
        "population": _proxy({"DE": 50.0, "FR": 50.0}),
        "gdp": _proxy({"DE": 90.0, "FR": 10.0}),
        "heating_demand": _proxy({"DE": 20.0, "FR": 60.0}),
    }
    weights = {"AC": {"gdp": 0.5, "population": 0.5}, "resistive": {"heating_demand": 1.0}}
    out = disaggregate_demand_to_country(
        load, {"EUR": ["DE", "FR"]}, proxies, weights, {"DE", "FR"}
    ).set_index(["sector", "region"])["value"]
    # AC: 0.5*gdp(0.9/0.1) + 0.5*pop(0.5/0.5) -> DE 0.7, FR 0.3
    assert out[("AC", "DE")] == pytest.approx(70.0)
    assert out[("AC", "FR")] == pytest.approx(30.0)
    # heating: heating-demand proxy 20:60 -> DE 0.25, FR 0.75 (GDP/pop ignored)
    assert out[("resistive", "DE")] == pytest.approx(25.0)
    assert out[("resistive", "FR")] == pytest.approx(75.0)


# -- build_demand_proxy_from_dd: population × degree-days (extensive demand proxy) -----


def test_build_demand_proxy_population_weighted_shares():
    """Intensity (degree-days) × size (population) gives a sensible extensive split."""
    dd = _proxy({"CN": 700.0, "TW": 900.0})   # TW slightly hotter (intensity)
    pop = _proxy({"CN": 1400.0, "TW": 23.0})  # CN far larger (size)
    scaled = build_demand_proxy_from_dd(dd, pop)
    assert scaled.loc[("CN", 2060), "value"] == pytest.approx(700.0 * 1400.0)
    assert scaled.loc[("TW", 2060), "value"] == pytest.approx(900.0 * 23.0)
    shares = normalise(scaled["value"])
    # Despite TW's higher CDD intensity, mainland dominates once size is included.
    assert shares.loc[("CN", 2060)] > 0.97


def test_build_demand_proxy_nearest_year_alignment():
    # degree-days at 2060 only; population at 2050 & 2060 -> uses 2060 (nearest).
    dd = _proxy({"CN": 700.0}, year=2060)
    pop = pd.DataFrame(
        {"iso2": ["CN", "CN"], "year": [2050, 2060], "value": [1000.0, 1400.0]}
    ).set_index(["iso2", "year"])
    scaled = build_demand_proxy_from_dd(dd, pop)
    assert scaled.loc[("CN", 2060), "value"] == pytest.approx(700.0 * 1400.0)


# -- read_degree_days ----------------------------------------------------------


def test_read_degree_days_iso3_to_iso2_and_filter():
    df = read_degree_days(CDD, dd_type="CDD", tlim_setpoint=22, rcp="4_5", ssp="SSP2")
    assert list(df.columns) == ["iso2", "year", "value"]
    idx = df.set_index(["iso2", "year"]).index
    assert ("DE", 2060) in idx and ("AT", 2060) in idx  # Germany + Austria both present
    assert (df["value"] >= 0).all()


def test_read_degree_days_bad_selector_raises():
    with pytest.raises(ValueError):  # out-of-range setpoint → no rows
        read_degree_days(CDD, dd_type="CDD", tlim_setpoint=99, rcp="4_5", ssp="SSP2")
    with pytest.raises(ValueError):  # unknown type
        read_degree_days(CDD, dd_type="XDD", tlim_setpoint=22, rcp="4_5", ssp="SSP2")


def test_iamc_symbols_include_space_cooling():
    """The Space Cooling FE variable is mapped to the `space_cooling` sector token."""
    from iampypsa.io.remind_symbols import load_symbol_specs

    variables = load_symbol_specs(backend="iamc")["demand_fe_sectors"]["variables"]
    key = "FE|Buildings|non-Heating|Electricity|Space Cooling"
    assert variables.get(key) == "space_cooling"
