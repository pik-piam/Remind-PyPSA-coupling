"""Pytest configuration and fixtures for REMIND-PyPSA coupling tests."""
import pytest
import pandas as pd


@pytest.fixture
def sample_remind_csv_data():
    """Sample REMIND CSV data for testing."""
    return pd.DataFrame({
        'ttot': [2030, 2035, 2040],
        'all_regi': ['EUR', 'USA', 'CHA'],
        'all_te': ['wind', 'solar', 'nuclear'],
        'value': [100.5, 200.3, 150.7]
    })


@pytest.fixture
def sample_pypsa_costs_data():
    """Sample PyPSA costs data for testing."""
    return pd.DataFrame({
        'technology': ['wind', 'solar', 'nuclear'],
        'year': [2030, 2030, 2030],
        'parameter': ['investment', 'investment', 'investment'],
        'value': [1200, 800, 5000],
        'unit': ['USD/MW', 'USD/MW', 'USD/MW'],
        'source': ['test', 'test', 'test']
    })


@pytest.fixture
def sample_tech_mapping():
    """Sample technology mapping data."""
    return pd.DataFrame({
        'PyPSA_tech': ['wind', 'solar', 'nuclear'],
        'parameter': ['investment', 'investment', 'investment'],
        'mapper': ['use_remind', 'use_remind', 'use_pypsa'],
        'reference': ['wind', 'solar', 'nuclear'],
        'unit': ['USD/MW', 'USD/MW', 'USD/MW'],
        'comment': ['', '', '']
    })


@pytest.fixture
def sample_capacities_data():
    """Sample capacity data for testing."""
    return pd.DataFrame({
        'year': [2030, 2035, 2035, 2035],
        'tech_group': ['wind', 'solar', 'coal', 'coal'],
        'tech_group': ['wind', 'solar', 'coal', 'coal'],
        'Capacity': [1000.0, 800.0, 1200.0, 900.0]
    })

@pytest.fixture
def sample_tech_map():
    """Sample technology map for testing."""
    tech_mapping = pd.DataFrame({
        'PyPSA_tech': ['wind_onshore', 'solar', "gas", "gas"],
        'parameter': ['investment', 'investment', 'investment', 'investment'],
        'mapper': ['use_remind', 'use_pypsa', "use_remind", "use_remind"],
        'reference': ['wind', 'spv', "gas1", "gas2"]
    })
    return tech_mapping

@pytest.fixture
def sample_pypsa_capacities():
    """Sample PyPSA capacity data."""
    return pd.DataFrame({
        'Tech': ['wind', 'coal',  'solar'],
        'Fueltype': ['wind', 'coal', 'solar'],
        'Capacity': [500.0, 600.0, 500.0],
        'tech_group': ['wind', 'coal', 'solar'],
        "grouping_years": ["2030", "2030", "2030"]
    })


@pytest.fixture
def sample_remind_capacities():
    """Sample REMIND capacity data for reference."""
    return pd.DataFrame({
        'technology': ['windon', 'spv', "coal"],
        'capacity': [800.0, 600.0, 0],
        'tech_group': ['wind', 'solar', "coal"],
        'year': [2030, 2030, 2030]
    })


@pytest.fixture
def tmp_csv_file(tmp_path, sample_remind_csv_data):
    """Create temporary CSV file with sample data."""
    file_path = tmp_path / "test_data.csv"
    sample_remind_csv_data.to_csv(file_path, index=False)
    return file_path


@pytest.fixture
def mock_region_mapping(tmp_path):
    """Mock region mapping CSV file."""
    data = {
        "region": ["USA", "EUR", "EUR"],
        "iso": ["USA", "DEU", "FRA"],
        "element_text": ["United States", "Germany", "France"],
    }
    df = pd.DataFrame(data)
    file_path = tmp_path / "region_mapping.csv"
    df.to_csv(file_path, index=False)
    return file_path