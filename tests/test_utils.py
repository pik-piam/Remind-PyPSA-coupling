import os
import pandas as pd
import pytest

from utils import read_remind_regions


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
