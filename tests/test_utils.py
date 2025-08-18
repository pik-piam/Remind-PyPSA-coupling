
"""Tests for rpycpl.utils module."""
import pandas as pd
import pytest

from rpycpl.utils import (
    read_remind_regions_csv as read_remind_regions,
    read_remind_csv,
    read_pypsa_costs,
    build_tech_map,
    expand_years,
    to_list,
    _fix_repeated_columns,
    REMIND_NAME_MAP
)


@pytest.fixture
def mock_csv_file(tmp_path):
    """Fixture to create a mock CSV file for testing."""
    data = {
        "region": ["USA", "EUR", "EUR"],
        "iso": ["USA", "DEU", "FRA"],
        "element_text": ["United States", "Germany", "France"],
    }
    df = pd.DataFrame(data)
    file_path = tmp_path / "mock_mapping.csv"
    df.to_csv(file_path, index=False)
    return file_path


def test_read_remind_regions(mock_csv_file):
    """Test the read_remind_regions function."""
    result = read_remind_regions(mock_csv_file)

    # Check that the result is a DataFrame
    assert isinstance(result, pd.DataFrame)

    # Check that the 'element_text' column is dropped
    assert "element_text" not in result.columns
    # Check that the 'iso2' column is added
    assert "iso2" in result.columns

    # Check the conversion to ISO2 codes
    expected_iso2 = ["US", "DE", "FR"]
    assert result["iso2"].tolist() == expected_iso2


def test_read_remind_csv(tmp_path):
    """Test reading REMIND CSV files with column mapping."""
    # Create sample data with REMIND column names
    data = {
        "ttot": [2030, 2035, 2040],
        "all_regi": ["EUR", "USA", "CHA"],
        "all_te": ["wind", "solar", "nuclear"],
        "value": [100.5, 200.3, 150.7]
    }
    df = pd.DataFrame(data)
    file_path = tmp_path / "remind_data.csv"
    df.to_csv(file_path, index=False)
    
    result = read_remind_csv(file_path)
    
    # Check column mapping
    assert "year" in result.columns
    assert "region" in result.columns
    assert "technology" in result.columns
    assert "ttot" not in result.columns
    assert "all_regi" not in result.columns
    assert "all_te" not in result.columns
    
    # Check data integrity
    assert len(result) == 3
    assert result["value"].dtype == float


def test_read_remind_csv_with_duplicate_columns(tmp_path):
    """Test reading REMIND CSV files with duplicate column suffixes."""
    data = {
        "all_te_1": ["wind", "solar", "nuclear"],
        "all_te_2": ["onshore", "pv", "gen3"],
        "value": [100.5, 200.3, 150.7]
    }
    df = pd.DataFrame(data)
    file_path = tmp_path / "remind_dup.csv"
    df.to_csv(file_path, index=False)
    
    result = read_remind_csv(file_path)
    
    # Check that suffixes are removed
    assert "technology" in result.columns
    assert "technology_1" in result.columns
    

def test_read_pypsa_costs(tmp_path):
    """Test reading and stitching PyPSA cost files."""
    # Create first cost file
    cost_data_1 = pd.DataFrame({
        'technology': ['wind', 'solar'],
        'year': [2030, 2030],
        'parameter': ['investment', 'investment'],
        'value': [1200, 800]
    })
    file1 = tmp_path / "costs_2030.csv"
    cost_data_1.to_csv(file1, index=False)
    
    # Create second cost file
    cost_data_2 = pd.DataFrame({
        'technology': ['wind', 'solar'],
        'year': [2035, 2035],
        'parameter': ['investment', 'investment'],
        'value': [1100, 750]
    })
    file2 = tmp_path / "costs_2035.csv"
    cost_data_2.to_csv(file2, index=False)
    
    result = read_pypsa_costs([str(file1), str(file2)])
    
    # Check that files are combined
    assert len(result) == 4
    assert set(result['year'].unique()) == {2030, 2035}
    assert set(result['technology'].unique()) == {'wind', 'solar'}


def test_build_tech_map():
    """Test building technology mapping from REMIND to PyPSA."""
    mapping_data = pd.DataFrame({
        'PyPSA_tech': ['wind_onshore', 'solar_pv', 'wind_onshore'],
        'parameter': ['investment', 'investment', 'VOM'],
        'mapper': ['use_remind', 'use_remind', 'use_remind'],
        'reference': ['wind', 'solar', 'wind']
    })
    
    result = build_tech_map(mapping_data, map_param='investment')
    
    # Check structure
    assert isinstance(result, pd.DataFrame)
    assert 'PyPSA_tech' in result.columns
    assert 'group' in result.columns
    assert result.index.name == 'remind_tech'
    
    # Check mapping logic
    assert 'wind' in result.index
    assert 'solar' in result.index


def test_build_tech_map_missing_parameter():
    """Test building tech map with missing parameter raises error."""
    mapping_data = pd.DataFrame({
        'PyPSA_tech': ['wind_onshore'],
        'parameter': ['investment'],
        'mapper': ['use_remind'],
        'reference': ['wind']
    })
    
    with pytest.raises(ValueError, match="Parameter nonexistent not found"):
        build_tech_map(mapping_data, map_param='nonexistent')


def test_expand_years():
    """Test expanding DataFrame to include missing years."""
    data = pd.DataFrame({
        'tech': ['onwind', 'offwind'],
        'value': [100, 150]
    })
    
    years = [2030, 2035, 2040, 2045]
    print(data)

    result = expand_years(data, years)
    print(set(result['year'].unique()))
    assert 'year' in result.columns, "Year column should be present"
    assert set(result['year'].unique()) == set(years)
    assert len(result) == len(years)*data.tech.nunique()  # 4 years for 2 techs


def test_to_list():
    """Test converting string representations of lists to lists."""
    # Test string representation of list
    assert to_list("[a, b, c]") == ['a', 'b', 'c']

    # Test single string
    assert to_list("single") == 'single'


def test_fix_repeated_columns():
    """Test fixing repeated column names."""
    cols = ['a', 'b', 'a', 'c', 'b', 'a']
    result = _fix_repeated_columns(cols)
    expected = ['a', 'b', 'a_1', 'c', 'b_1', 'a_2']
    assert result == expected


def test_remind_name_map():
    """Test REMIND column name mapping constants."""
    assert REMIND_NAME_MAP['ttot'] == 'year'
    assert REMIND_NAME_MAP['all_regi'] == 'region'
    assert REMIND_NAME_MAP['all_te'] == 'technology'


class TestRegistryDecorators:
    """Test the decorator registry functionality."""
    
    def test_register_reader_decorator(self):
        """Test that register_reader decorator works."""
        from rpycpl.utils import register_reader, READERS_REGISTRY
        
        @register_reader("test_reader")
        def dummy_reader(file_path):
            return pd.DataFrame()
            
        assert "test_reader" in READERS_REGISTRY
        assert READERS_REGISTRY["test_reader"] == dummy_reader
