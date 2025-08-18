"""Tests for rpycpl.capacities_etl module."""
import pandas as pd
import pytest
import logging

from rpycpl.capacities_etl import (
    scale_down_capacities,
    calc_paidoff_capacity
)
logger = logging.getLogger(__name__)


class TestScaleDownCapacities:
    """Test cases for scale_down_capacities function (harmonisation)."""

    def test_scale_down_basic(self, sample_pypsa_capacities, sample_remind_capacities):
        """Test basic scaling down functionality."""
        # Modify the fixture data for this specific test
        to_scale = sample_pypsa_capacities.copy()
        to_scale['Capacity'] = [700.0, 400.0, 350.0]  # Total: wind=1000, solar=500
        logger.debug("To scale DataFrame:\n%s", to_scale)
        reference = sample_remind_capacities.copy()
        reference['capacity'] = [300.0, 350.0, 0]  # Reference caps lower than to_scale totals
        logger.debug("Reference DataFrame:\n%s", reference)

        result = scale_down_capacities(to_scale, reference)
        logger.debug("Scaled down DataFrame:\n%s", result)
        
        # Check that capacities are scaled down proportionally
        capacities = result.groupby('tech_group')['Capacity'].sum()
        logger.debug("Scaled down DataFrame:\n%s", capacities)
        wind_total = capacities['wind']
        solar_total = capacities['solar']
        coal_total = capacities['coal']
        
        logger.debug(wind_total, solar_total, coal_total)

        assert abs(wind_total - 300.0) < 1e-6  # Should be scaled to reference
        assert abs(solar_total - 350.0) < 1e-6  # Should be scaled to reference
        assert abs(coal_total - 0.0) < 1e-6  # Coal should be zero
        
        # Check original capacity is preserved
        assert 'original_capacity' in result.columns
        assert result['original_capacity'].sum() == 1450.0  # Original total

    def test_scale_down_no_scaling_needed(self, sample_pypsa_capacities):
        """Test when reference capacity is higher than existing capacity."""
        to_scale = pd.DataFrame({
            'Tech': ['wind', 'solar'],
            'Fueltype': ['wind', 'solar'],
            'Capacity': [500.0, 300.0],
            'tech_group': ['wind', 'solar']
        })
        
        reference = pd.DataFrame({
            'technology': ['wind', 'solar'],
            'capacity': [1000.0, 600.0],  # Higher than existing
            'tech_group': ['wind', 'solar'],
            'year': [2030, 2030]
        })
        
        result = scale_down_capacities(to_scale, reference)
        
        # Capacities should remain unchanged
        assert result[result['tech_group'] == 'wind']['Capacity'].iloc[0] == 500.0
        assert result[result['tech_group'] == 'solar']['Capacity'].iloc[0] == 300.0

    def test_scale_down_missing_tech_groups(self):
        """Test handling of missing tech groups in reference data."""
        to_scale = pd.DataFrame({
            'Tech': ['wind', 'nuclear'],
            'Fueltype': ['wind', 'nuclear'],
            'Capacity': [500.0, 300.0],
            'tech_group': ['wind', 'nuclear']
        })
        
        reference = pd.DataFrame({
            'technology': ['wind'],  # Missing nuclear
            'capacity': [400.0],
            'tech_group': ['wind'],
            'year': [2030]
        })
        
        result = scale_down_capacities(to_scale, reference)
        
        # Wind should be scaled, nuclear should go to zero
        assert result[result['tech_group'] == 'wind']['Capacity'].iloc[0] == 400.0
        assert result[result['tech_group'] == 'nuclear']['Capacity'].iloc[0] == 0.0

    def test_scale_down_multiple_years_error(self):
        """Test that multiple years in reference raises error."""
        to_scale = pd.DataFrame({
            'Tech': ['wind'],
            'Fueltype': ['wind'],
            'Capacity': [500.0],
            'tech_group': ['wind']
        })
        
        reference = pd.DataFrame({
            'technology': ['wind', 'wind'],
            'capacity': [400.0, 500.0],
            'tech_group': ['wind', 'wind'],
            'year': [2030, 2035]  # Multiple years
        })
        
        with pytest.raises(ValueError, match="single year"):
            scale_down_capacities(to_scale, reference)

    def test_scale_down_empty_tech_group_warning(self):
        """Test warning for empty tech groups."""
        to_scale = pd.DataFrame({
            'Tech': ['wind'],
            'Fueltype': ['wind'],
            'Capacity': [500.0],
            'tech_group': ['']  # Empty tech group
        })
        
        reference = pd.DataFrame({
            'technology': ['wind'],
            'capacity': [400.0],
            'tech_group': ['wind'],
            'year': [2030]
        })
        
        # Should handle empty tech groups gracefully
        result = scale_down_capacities(to_scale, reference)
        assert len(result) == 0  # Empty tech groups filtered out


class TestCalcPaidoffCapacity:
    """Test cases for calc_paidoff_capacity function."""

    def test_calc_paidoff_basic(self):
        """Test basic paid-off capacity calculation."""
        remind_capacities = pd.DataFrame({
            'tech_group': ['wind', 'solar', 'wind', 'solar'],
            'year': [2030, 2030, 2035, 2035],
            'capacity': [1000.0, 800.0, 1200.0, 900.0]
        })
        
        # Harmonized PyPSA caps (what's actually allocated)
        scaled_2030 = pd.DataFrame({
            'Tech': ['wind', 'solar'],
            'Capacity': [600.0, 500.0],
            'tech_group': ['wind', 'solar']
        })
        
        scaled_2035 = pd.DataFrame({
            'Tech': ['wind', 'solar'],
            'Capacity': [800.0, 600.0],
            'tech_group': ['wind', 'solar']
        })
        
        scaled_pypsa_caps = {
            2030: scaled_2030,
            2035: scaled_2035
        }
        
        result = calc_paidoff_capacity(remind_capacities, scaled_pypsa_caps)
        
        # Check structure
        assert isinstance(result, pd.DataFrame)
        assert set(result.columns) == {'tech_group', 'year', 'Capacity'}
        
        # Check calculations
        # 2030: wind=1000-600=400, solar=800-500=300
        # 2035: wind=1200-800=400, solar=900-600=300
        wind_2030 = result.query("tech_group == 'wind' and year == 2030")['Capacity'].iloc[0]
        solar_2030 = result.query("tech_group == 'solar' and year == 2030")['Capacity'].iloc[0]
        
        assert wind_2030 == 400.0
        assert solar_2030 == 300.0

    def test_calc_paidoff_no_excess_capacity(self):
        """Test when PyPSA capacity equals REMIND capacity."""
        remind_capacities = pd.DataFrame({
            'tech_group': ['wind'],
            'year': [2030],
            'capacity': [1000.0]
        })
        
        scaled_pypsa_caps = {
            2030: pd.DataFrame({
                'Tech': ['wind'],
                'Capacity': [1000.0],  # Equal to REMIND
                'tech_group': ['wind']
            })
        }
        
        result = calc_paidoff_capacity(remind_capacities, scaled_pypsa_caps)
        
        # Should have zero paid-off capacity
        assert result[result['tech_group'] == 'wind']['Capacity'].iloc[0] == 0.0

    def test_calc_paidoff_negative_capacity_error(self):
        """Test error when harmonized capacity exceeds REMIND capacity."""
        remind_capacities = pd.DataFrame({
            'tech_group': ['wind'],
            'year': [2030],
            'capacity': [500.0]  # Less than harmonized
        })
        
        scaled_pypsa_caps = {
            2030: pd.DataFrame({
                'Tech': ['wind'],
                'Capacity': [600.0],  # More than REMIND
                'tech_group': ['wind']
            })
        }
        
        with pytest.raises(ValueError, match="negative Paid off capacities"):
            calc_paidoff_capacity(remind_capacities, scaled_pypsa_caps)

    def test_calc_paidoff_empty_harmonized_caps(self):
        """Test handling of empty harmonized capacity data."""
        remind_capacities = pd.DataFrame({
            'tech_group': ['wind'],
            'year': [2030],
            'capacity': [1000.0]
        })
        
        scaled_pypsa_caps = {
            2030: pd.DataFrame()  # Empty DataFrame
        }
        
        with pytest.raises(ValueError, match="No harmonized capacities provided for any year."):
            calc_paidoff_capacity(remind_capacities, scaled_pypsa_caps)
   

    def test_calc_paidoff_multiple_tech_groups(self):
        """Test with multiple technology groups."""
        remind_capacities = pd.DataFrame({
            'tech_group': ['wind', 'solar', 'nuclear'],
            'year': [2030, 2030, 2030],
            'capacity': [1000.0, 800.0, 500.0]
        })
        
        scaled_pypsa_caps = {
            2030: pd.DataFrame({
                'Tech': ['wind', 'solar', 'nuclear'],
                'Capacity': [700.0, 600.0, 500.0],
                'tech_group': ['wind', 'solar', 'nuclear']
            })
        }
        
        result = calc_paidoff_capacity(remind_capacities, scaled_pypsa_caps)
        
        # Check all tech groups are present
        tech_groups = set(result['tech_group'])
        expected_groups = {'wind', 'solar', 'nuclear'}
        assert tech_groups == expected_groups
        
        # Check individual calculations
        wind_paid = result[result['tech_group'] == 'wind']['Capacity'].iloc[0]
        solar_paid = result[result['tech_group'] == 'solar']['Capacity'].iloc[0]
        nuclear_paid = result[result['tech_group'] == 'nuclear']['Capacity'].iloc[0]
        
        assert wind_paid == 300.0  # 1000 - 700
        assert solar_paid == 200.0  # 800 - 600
        assert nuclear_paid == 0.0  # 500 - 500
