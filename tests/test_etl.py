"""Tests for rpycpl.etl module"""
import pandas as pd
import logging 

from rpycpl.etl import (
    Transformation,
    register_etl,
    ETL_REGISTRY,
    build_tech_groups,
    convert_loads,
    convert_remind_capacities
)

logger = logging.getLogger(__name__)
TWYR2MWH = 365 * 24 * 1e6
TW2MW = 1e6

class TestTransformationClass:
    """Test cases for the Transformation dataclass."""

    def test_transformation_creation(self):
        """Test basic Transformation creation."""
        transform = Transformation(
            name="test_transform",
            method="test_method",
        )
        
        assert transform.name == "test_transform"
        assert transform.method == "test_method"
        assert isinstance(transform.frames, dict)
        assert isinstance(transform.params, dict)
        assert isinstance(transform.filters, dict)
        assert isinstance(transform.kwargs, dict)
        assert isinstance(transform.dependencies, dict)

    def test_transformation_with_all_fields(self):
        """Test Transformation with all fields populated."""
        transform = Transformation(
            name="full_transform",
            method="convert_loads",
            frames={"ac_load": "p32_load"},
            params={"region": "CHA"},
            filters={"load": "region==@region"},
            kwargs={"cutoff": 0.1},
            dependencies={"tech_mapping": "mapping.csv"}
        )
        
        assert transform.name == "full_transform"
        assert transform.frames["ac_load"] == "p32_load"
        assert transform.params["region"] == "CHA"
        assert transform.filters["load"] == "region==@region"
        assert transform.kwargs["cutoff"] == 0.1
        assert transform.dependencies["tech_mapping"] == "mapping.csv"


class TestETLRegistry:
    """Test cases for ETL registry and decorator."""

    def test_register_etl_decorator(self):
        """Test that register_etl decorator works."""
        # Register a test function
        @register_etl("test_etl_function")
        def test_function(data):
            return data
        
        # Check it's in registry
        assert "test_etl_function" in ETL_REGISTRY
        assert ETL_REGISTRY["test_etl_function"] == test_function

    def test_registry_has_built_in_functions(self):
        """Test that built-in ETL functions are registered."""
        expected_functions = [
            "build_tech_map",
            "convert_load",
            "convert_capacities"
        ]
        
        for func_name in expected_functions:
            assert func_name in ETL_REGISTRY


def test_build_tech_groups(sample_tech_map):
    """Test basic tech groups building."""
    tech_mapping = sample_tech_map.copy()

    frames = {"tech_mapping": tech_mapping}
    result = build_tech_groups(frames)
    logger.info(result)
    # Should return the tech mapping result
    assert isinstance(result, pd.DataFrame)
    assert 'PyPSA_tech' in result.columns
    assert 'group' in result.columns

    ngroups_exp = tech_mapping[tech_mapping["mapper"].str.contains('remind', case=False)]["PyPSA_tech"].nunique()
    assert result["group"].nunique() == ngroups_exp


class TestConvertLoads:
    """Test cases for convert_loads function."""

    def test_convert_loads_basic(self):
        """Test basic load conversion."""
        ac_load = pd.DataFrame({
            'year': [2030, 2035, 2030],
            'region': ['CHA', 'CHA', "EUR"],
            'value': [1.0, 1.2, 0.1]  # TWh/yr
        })
        
        h2_load = pd.DataFrame({
            'year': [2030, 2035],
            'region': ['CHA', 'CHA'],
            'value': [0.5, 0.6]  # TWh/yr
        })
        
        loads = {
            'ac_load': ac_load,
            'h2_el_load': h2_load
        }
        
        result = convert_loads(loads, region='CHA')
        
        # Check structure
        assert isinstance(result, pd.DataFrame)
        assert result.index.name == 'year'
        assert 'load' in result.columns
        assert 'value' in result.columns
        
        # Check conversion (TWh/yr to MWh)
        ac_2035 = result[(result.index == 2035) & (result['load'] == 'ac')]['value'].iloc[0]
        assert abs(ac_2035 - 1.2 * TWYR2MWH) < 1e-6
        # check EUR excluded
        assert "region" not in result.columns  # Should not have region column after conversion
        ac_2030 = result[(result.index == 2030) & (result['load'] == 'ac')]['value'].iloc[0]
        assert abs(ac_2030 - 1.0 * TWYR2MWH) < 1e-6

    def test_convert_loads_no_region_filter(self):
        """Test load conversion without region filtering."""
        ac_load = pd.DataFrame({
            'year': [2030, 2035],
            'value': [1.0, 1],  # No region column
        })
        
        loads = {'ac_load': ac_load}
        
        result = convert_loads(loads)
        logger.info(result)
        assert len(result) == 2
        assert set(result.index) == {2030, 2035}

    def test_convert_loads_multiple_regions(self):
        """Test load conversion with region filtering."""
        ac_load = pd.DataFrame({
            'year': [2030, 2030, 2035, 2040],
            'region': ['CHA', 'EUR', 'CHA', 'EUR'],
            'value': [1.0, 1.5, 1.2, 1.8]
        })
        
        loads = {'ac_load': ac_load}
        
        result = convert_loads(loads)
        
        # Should only have China and Europe data
        assert len(result) == 4
        assert all(result.index.isin([2030, 2035, 2040]))


class TestConvertRemindCapacities:
    """Test cases for convert_remind_capacities function."""

    def test_convert_capacities_basic(self):
        """Test basic capacity conversion."""
        capacities = pd.DataFrame({
            'year': [2030, 2035],
            'technology': ['wind', 'solar'],
            'region': ['CHA', 'CHA'],
            'value': [1.0, 0.8]  # TW
        })
        
        frames = {'capacities': capacities}
        result = convert_remind_capacities(frames, region='CHA')
        
        # Check TW to MW conversion
        logger.info(result)
        assert result.query("technology=='wind'")['capacity'].iloc[0] == 1.0 * TW2MW

    def test_convert_capacities_with_cutoff(self):
        """Test capacity conversion with cutoff."""
        capacities = pd.DataFrame({
            'year': [2030, 2030],
            'technology': ['wind', 'solar'],
            'value': [1000.0, 0.5/TW2MW]  # MW, one below cutoff
        })
        
        frames = {'capacities': capacities}
        
        result = convert_remind_capacities(frames, cutoff=1.0)
        
        # Solar should be set to 0 due to cutoff
        logger.info(result)
        solar_cap = result.query("technology == 'solar'").iloc[0]['capacity']
        logger.info(f"Solar capacity after cutoff: {solar_cap}")
        assert solar_cap == 0

    def test_convert_capacities_with_tech_groups(self):
        """Test capacity conversion with tech groups."""
        capacities = pd.DataFrame({
            'year': [2030],
            'technology': ['windon'],
            'value': [1000.0]
        })
        
        tech_groups = pd.DataFrame({
            'remind_tech': ['windon'],
            'PyPSA_tech': ['onwind'],
            'group': ['wind']
        })
        tech_groups.set_index('remind_tech', inplace=True)
        
        frames = {
            'capacities': capacities,
            'tech_groups': tech_groups
        }
        
        result = convert_remind_capacities(frames)
        
        # Should have tech_group column
        assert 'tech_group' in result.columns
        assert result['tech_group'].iloc[0] == 'wind'
