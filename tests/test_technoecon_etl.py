"""Tests for rpycpl.technoecon_etl module."""
import pandas as pd
import pytest

from rpycpl.technoecon_etl import (
    validate_mappings,
    validate_remind_data,
    to_list,
    UNIT_CONVERSION,
    REMIND_PARAM_MAP,
    MAPPING_FUNCTIONS,
    OUTP_COLS
)


class TestConstants:
    """Test constants and configuration."""

    def test_unit_conversion_constants(self):
        """Test unit conversion constants are reasonable."""
        assert UNIT_CONVERSION['capex'] == 1e6  # TUSD/TW to USD/MW
        assert UNIT_CONVERSION['VOM'] == 1e6 / 8760  # TUSD/TWa to USD/MWh
        assert UNIT_CONVERSION['FOM'] == 100  # p.u to percent
        assert UNIT_CONVERSION['currency'] == 1
        
        # CO2 intensity conversion should be positive
        assert UNIT_CONVERSION['co2_intensity'] > 0

    def test_remind_param_map(self):
        """Test REMIND parameter mapping."""
        expected_keys = [
            'tech_data', 'capex', 'eta', 'eta_part2',
            'fuel_costs', 'discount_r', 'co2_intensity', 'weights_gen'
        ]
        
        for key in expected_keys:
            assert key in REMIND_PARAM_MAP
            assert isinstance(REMIND_PARAM_MAP[key], str)

    def test_mapping_functions_list(self):
        """Test mapping functions list."""
        expected_functions = [
            "set_value", "use_remind", "use_remind_with_learning_from",
            "use_pypsa", "weigh_remind_by_gen", "weigh_remind_by_capacity"
        ]
        
        for func in expected_functions:
            assert func in MAPPING_FUNCTIONS

    def test_output_columns(self):
        """Test output columns definition."""
        expected_cols = [
            'technology', 'year', 'parameter', 'value',
            'unit', 'source', 'further description'
        ]
        
        assert OUTP_COLS == expected_cols


class TestValidateMappings:
    """Test cases for validate_mappings function."""

    def test_validate_mappings_valid(self):
        """Test validation of valid mappings."""
        mappings = pd.DataFrame({
            'PyPSA_tech': ['wind_onshore', 'solar_pv'],
            'parameter': ['investment', 'investment'],
            'mapper': ['use_remind', 'set_value'],
            'reference': ['wind', '1200'],
            'unit': ['USD/MW', 'USD/MW'],
            'comment': ['', 'Fixed cost']
        })
        
        # Should not raise any exception for valid mappings
        validate_mappings(mappings)

    def test_validate_mappings_invalid_mapper(self):
        """Test validation fails for invalid mapper."""
        mappings = pd.DataFrame({
            'PyPSA_tech': ['wind_onshore'],
            'parameter': ['investment'],
            'mapper': ['invalid_mapper'],  # Invalid
            'reference': ['wind'],
            'unit': ['USD/MW'],
            'comment': ['']
        })
        
        with pytest.raises(ValueError, match="invalid_mapper"):
            validate_mappings(mappings)

    def test_validate_mappings_missing_columns(self):
        """Test validation fails for missing required columns."""
        mappings = pd.DataFrame({
            'PyPSA_tech': ['wind_onshore'],
            'parameter': ['investment'],
            # Missing 'mapper' column
            'reference': ['wind']
        })
        
        with pytest.raises(ValueError):
            validate_mappings(mappings)

    def test_validate_mappings_empty_dataframe(self):
        """Test validation of empty mappings dataframe."""
        mappings = pd.DataFrame()
        
        # Should handle empty dataframe gracefully or raise appropriate error
        with pytest.raises((ValueError, KeyError)):
            validate_mappings(mappings)


class TestValidateRemindData:
    """Test cases for validate_remind_data function."""

    def test_validate_remind_data_valid(self):
        """Test validation of valid REMIND data."""
        costs_remind = pd.DataFrame({
            'technology': ['wind', 'solar'],
            'year': [2030, 2030],
            'parameter': ['investment', 'investment'],
            'value': [1200, 800]
        })
        
        mappings = pd.DataFrame({
            'PyPSA_tech': ['wind_onshore', 'solar_pv'],
            'parameter': ['investment', 'investment'],
            'mapper': ['use_remind', 'upse_pypsa'],
            'reference': ['wind', 'solar']
        })
        
        # Should not raise exception for valid data
        validate_remind_data(costs_remind, mappings)

    def test_validate_remind_data_missing_tech(self):
        """Test validation when REMIND data missing required technology."""
        costs_remind = pd.DataFrame({
            'technology': ['wind'],  # Missing solar
            'year': [2030],
            'parameter': ['investment'],
            'value': [1200]
        })
        
        mappings = pd.DataFrame({
            'PyPSA_tech': ['wind_onshore', 'solar_pv'],
            'parameter': ['investment', 'investment'],
            'mapper': ['use_remind', 'use_remind'],
            'reference': ['wind', 'solar']  # Solar required but missing
        })
        
        with pytest.raises(ValueError, match = "Missing data in REMIND"):
            validate_remind_data(costs_remind, mappings)

    def test_validate_remind_data_empty_costs(self):
        """Test validation with empty REMIND costs."""
        costs_remind = pd.DataFrame()
        
        mappings = pd.DataFrame({
            'PyPSA_tech': ['wind_onshore'],
            'parameter': ['investment'],
            'mapper': ['use_remind'],
            'reference': ['wind']
        })
        
        # Should handle empty costs data
        with pytest.raises(ValueError, match = "Remind data does not have"):
            validate_remind_data(costs_remind, mappings)


class TestETLIntegration:
    """Integration test scenarios for techno-econ ETL."""

    def test_mapping_validation_pipeline(self):
        """Test full mapping validation pipeline."""
        # Create realistic mapping data
        mappings = pd.DataFrame({
            'PyPSA_tech': ['wind_onshore', 'solar_pv', 'nuclear'],
            'parameter': ['investment', 'investment', 'investment'],
            'mapper': ['use_remind', 'use_remind', 'set_value'],
            'reference': ['wind', 'solar', '5000'],
            'unit': ['USD/MW', 'USD/MW', 'USD/MW'],
            'comment': ['', '', 'Fixed nuclear cost']
        })
        
        costs_remind = pd.DataFrame({
            'technology': ['wind', 'solar'],
            'year': [2030, 2030],
            'parameter': ['investment', 'investment'],
            'value': [1200, 800]
        })
        
        # Should validate both mappings and REMIND data
        validate_mappings(mappings)
        validate_remind_data(costs_remind, mappings)
