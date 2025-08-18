"""Integration tests for the REMIND-PyPSA coupling package."""
import pandas as pd
import pytest

from rpycpl.etl import Transformation, ETL_REGISTRY
from rpycpl.capacities_etl import scale_down_capacities, calc_paidoff_capacity


class TestWorkflowIntegration:
    """Integration tests for typical workflow scenarios."""

    def test_capacity_harmonization_workflow(self):
        """Test capacity harmonization workflow."""
        # 1. REMIND capacities
        remind_caps = pd.DataFrame({
            'tech_group': ['wind', 'solar'],
            'year': [2030, 2030],
            'capacity': [1000.0, 800.0]
        })
        
        # 2. PyPSA brownfield capacities (higher resolution)
        pypsa_caps = pd.DataFrame({
            'Tech': ['wind_onshore', 'wind_offshore', 'solar_pv', 'solar_thermal'],
            'Fueltype': ['wind', 'wind', 'solar', 'solar'],
            'Capacity': [400.0, 800.0, 600.0, 400.0],  # Total: wind=1200, solar=1000
            'tech_group': ['wind', 'wind', 'solar', 'solar']
        })
        
        # 3. Scale down PyPSA capacities to REMIND levels
        scaled_caps = scale_down_capacities(pypsa_caps, remind_caps)
        
        # 4. Verify scaling
        wind_total = scaled_caps[scaled_caps['tech_group'] == 'wind']['Capacity'].sum()
        solar_total = scaled_caps[scaled_caps['tech_group'] == 'solar']['Capacity'].sum()
        
        assert abs(wind_total - 1000.0) < 1e-6  # Scaled down from 1200 to 1000
        assert abs(solar_total - 800.0) < 1e-6   # Scaled down from 1000 to 800
        
        # 5. Calculate paid-off capacity
        harmonized_caps = {2030: scaled_caps}
        paid_off = calc_paidoff_capacity(remind_caps, harmonized_caps)
        
        # Should be zero since REMIND caps = scaled caps
        assert all(paid_off['Capacity'] == 0.0)

    def test_technoeconomic_data_workflow(self):
        """Test complete technoeconomic data transformation workflow."""
        # 1. tech mapping with different mappers
        # Map all relevant parameters (investment, VOM, FOM, lifetime, CO2 intensity, efficiency)
        mapping_data = pd.DataFrame([
            # Investment cost mappings
            {'PyPSA_tech': 'onwind', 'parameter': 'investment', 'mapper': 'use_remind', 'reference': 'windon', 'unit': 'USD/MW', 'comment': ''},
            {'PyPSA_tech': 'offwind', 'parameter': 'investment', 'mapper': 'use_remind', 'reference': 'windoff', 'unit': 'USD/MW', 'comment': ''},
            {'PyPSA_tech': 'solar', 'parameter': 'investment', 'mapper': 'use_remind', 'reference': 'spv', 'unit': 'USD/MW', 'comment': ''},
            {'PyPSA_tech': 'OCGT', 'parameter': 'investment', 'mapper': 'weigh_remind_by_gen', 'reference': ['gaschp', 'gascc'], 'unit': 'USD/MW', 'comment': 'Weighted by generation'},
            {'PyPSA_tech': 'nuclear', 'parameter': 'investment', 'mapper': 'set_value', 'reference': 5000, 'unit': 'USD/MW', 'comment': 'Fixed cost'},
            # VOM mappings
            {'PyPSA_tech': 'onwind', 'parameter': 'VOM', 'mapper': 'use_remind', 'reference': 'windon', 'unit': 'USD/MWh', 'comment': ''},
            {'PyPSA_tech': 'solar', 'parameter': 'VOM', 'mapper': 'use_remind', 'reference': 'spv', 'unit': 'USD/MWh', 'comment': ''},
            # FOM mappings
            {'PyPSA_tech': 'onwind', 'parameter': 'FOM', 'mapper': 'use_remind', 'reference': 'windon', 'unit': 'USD/kW/a', 'comment': ''},
            {'PyPSA_tech': 'solar', 'parameter': 'FOM', 'mapper': 'use_remind', 'reference': 'spv', 'unit': 'USD/kW/a', 'comment': ''},
            # Lifetime mappings
            {'PyPSA_tech': 'onwind', 'parameter': 'lifetime', 'mapper': 'use_remind', 'reference': 'windon', 'unit': 'a', 'comment': ''},
            {'PyPSA_tech': 'solar', 'parameter': 'lifetime', 'mapper': 'use_remind', 'reference': 'spv', 'unit': 'a', 'comment': ''},
            # CO2 intensity mappings
            {'PyPSA_tech': 'onwind', 'parameter': 'CO2 intensity', 'mapper': 'set_value', 'reference': 0, 'unit': 'tCO2/MWh', 'comment': ''},
            {'PyPSA_tech': 'offwind', 'parameter': 'CO2 intensity', 'mapper': 'set_value', 'reference': 0, 'unit': 'tCO2/MWh', 'comment': ''},
            {'PyPSA_tech': 'solar', 'parameter': 'CO2 intensity', 'mapper': 'set_value', 'reference': 0, 'unit': 'tCO2/MWh', 'comment': ''},
            # Efficiency mappings
            {'PyPSA_tech': 'onwind', 'parameter': 'efficiency', 'mapper': 'use_remind', 'reference': 'windon', 'unit': '', 'comment': ''},
            {'PyPSA_tech': 'solar', 'parameter': 'efficiency', 'mapper': 'use_remind', 'reference': 'spv', 'unit': '', 'comment': ''},
            {'PyPSA_tech': 'OCGT', 'parameter': 'efficiency', 'mapper': 'weigh_remind_by_gen', 'reference': ['gaschp', 'gascc'], 'unit': '', 'comment': 'Weighted by generation'},
        ])
        
        # 2. Create REMIND cost frames (what make_pypsa_like_costs expects)
        remind_frames = {
            # Capital costs
            'capex': pd.DataFrame({
                'technology': ['windon', 'windoff', 'spv', 'gaschp', 'gascc'],
                'year': [2030, 2030, 2030, 2030, 2030],
                'value': [1.2, 1.5, 0.8, 0.6, 0.9]  # TUSD/TW
            }),
            
            # Technical data (VOM, FOM, lifetime)
            'tech_data': pd.DataFrame([
                {'technology': 'windon', 'year': 2030, 'parameter': 'omv', 'value': 0.01},
                {'technology': 'windon', 'year': 2030, 'parameter': 'omf', 'value': 0.03},
                {'technology': 'windon', 'year': 2030, 'parameter': 'lifetime', 'value': 25},
                {'technology': 'spv', 'year': 2030, 'parameter': 'omv', 'value': 0.005},
                {'technology': 'spv', 'year': 2030, 'parameter': 'omf', 'value': 0.02},
                {'technology': 'spv', 'year': 2030, 'parameter': 'lifetime', 'value': 30},
                {'technology': 'gaschp', 'year': 2030, 'parameter': 'omv', 'value': 0.02},
                {'technology': 'gascc', 'year': 2030, 'parameter': 'omv', 'value': 0.015}
            ]),
            
            # CO2 intensity
            'co2_intensity': pd.DataFrame({
                'technology': ['windon', 'spv', 'gaschp', 'gascc'],
                "to_carrier": ['seel', 'seel', 'seel', 'seel'],
                'year': [2030, 2030, 2030, 2030],
                'value': [0.0, 0.0, 0.4, 0.35],  # Gt_C/TWa
                "emission_type": ['CO2', 'CO2', 'CO2', 'CO2']
            }),
            
            # Efficiency
            'eta': pd.DataFrame({
                'technology': ['windon', 'spv', 'gaschp', 'gascc'],
                'year': [2030, 2030, 2030, 2030],
                'value': [1.0, 1.0, 0.45, 0.55]
            }),
            
            # Fuel costs
            'fuel_costs': pd.DataFrame({
                'carrier': ['pegas'],
                'year': [2030],
                'value': [0.03]  # TUSD/TWa
            }),
            
            # Discount rate
            'discount_r': pd.DataFrame({
                'year': [2030],
                'value': [0.07]
            }),
            
            # Generation weights for weighted averaging
            'weights_gen': pd.DataFrame({
                'technology': ['gaschp', 'gascc'],
                'year': [2030, 2030],
                'value': [0.3, 0.7]  # gaschp gets 30%, gascc gets 70%
            })
        }
        
        # 3. Create PyPSA costs data
        pypsa_costs = pd.DataFrame([
            # Investment costs
            {'technology': 'onwind', 'year': 2030, 'parameter': 'investment', 'value': 1200, 'unit': 'USD/MW', 'source': 'pypsa', 'further description': ''},
            {'technology': 'offwind', 'year': 2030, 'parameter': 'investment', 'value': 1800, 'unit': 'USD/MW', 'source': 'pypsa', 'further description': ''},
            {'technology': 'solar', 'year': 2030, 'parameter': 'investment', 'value': 600, 'unit': 'USD/MW', 'source': 'pypsa', 'further description': ''},
            {'technology': 'OCGT', 'year': 2030, 'parameter': 'investment', 'value': 800, 'unit': 'USD/MW', 'source': 'pypsa', 'further description': ''},
            {'technology': 'nuclear', 'year': 2030, 'parameter': 'investment', 'value': 4500, 'unit': 'USD/MW', 'source': 'pypsa', 'further description': ''},
            # VOM (Variable O&M)
            {'technology': 'onwind', 'year': 2030, 'parameter': 'VOM', 'value': 10, 'unit': 'USD/MWh', 'source': 'pypsa', 'further description': ''},
            {'technology': 'offwind', 'year': 2030, 'parameter': 'VOM', 'value': 12, 'unit': 'USD/MWh', 'source': 'pypsa', 'further description': ''},
            {'technology': 'solar', 'year': 2030, 'parameter': 'VOM', 'value': 5, 'unit': 'USD/MWh', 'source': 'pypsa', 'further description': ''},
            {'technology': 'OCGT', 'year': 2030, 'parameter': 'VOM', 'value': 15, 'unit': 'USD/MWh', 'source': 'pypsa', 'further description': ''},
            {'technology': 'nuclear', 'year': 2030, 'parameter': 'VOM', 'value': 8, 'unit': 'USD/MWh', 'source': 'pypsa', 'further description': ''},
            # FOM (Fixed O&M)
            {'technology': 'onwind', 'year': 2030, 'parameter': 'FOM', 'value': 30, 'unit': 'USD/kW/a', 'source': 'pypsa', 'further description': ''},
            {'technology': 'offwind', 'year': 2030, 'parameter': 'FOM', 'value': 35, 'unit': 'USD/kW/a', 'source': 'pypsa', 'further description': ''},
            {'technology': 'solar', 'year': 2030, 'parameter': 'FOM', 'value': 20, 'unit': 'USD/kW/a', 'source': 'pypsa', 'further description': ''},
            {'technology': 'OCGT', 'year': 2030, 'parameter': 'FOM', 'value': 25, 'unit': 'USD/kW/a', 'source': 'pypsa', 'further description': ''},
            {'technology': 'nuclear', 'year': 2030, 'parameter': 'FOM', 'value': 50, 'unit': 'USD/kW/a', 'source': 'pypsa', 'further description': ''},
            # Lifetime
            {'technology': 'onwind', 'year': 2030, 'parameter': 'lifetime', 'value': 25, 'unit': 'a', 'source': 'pypsa', 'further description': ''},
            {'technology': 'offwind', 'year': 2030, 'parameter': 'lifetime', 'value': 25, 'unit': 'a', 'source': 'pypsa', 'further description': ''},
            {'technology': 'solar', 'year': 2030, 'parameter': 'lifetime', 'value': 30, 'unit': 'a', 'source': 'pypsa', 'further description': ''},
            {'technology': 'OCGT', 'year': 2030, 'parameter': 'lifetime', 'value': 35, 'unit': 'a', 'source': 'pypsa', 'further description': ''},
            {'technology': 'nuclear', 'year': 2030, 'parameter': 'lifetime', 'value': 40, 'unit': 'a', 'source': 'pypsa', 'further description': ''},
            # CO2 intensity
            {'technology': 'onwind', 'year': 2030, 'parameter': 'CO2 intensity', 'value': 0.0, 'unit': 'tCO2/MWh', 'source': 'pypsa', 'further description': ''},
            {'technology': 'offwind', 'year': 2030, 'parameter': 'CO2 intensity', 'value': 0.0, 'unit': 'tCO2/MWh', 'source': 'pypsa', 'further description': ''},
            {'technology': 'solar', 'year': 2030, 'parameter': 'CO2 intensity', 'value': 0.0, 'unit': 'tCO2/MWh', 'source': 'pypsa', 'further description': ''},
            {'technology': 'OCGT', 'year': 2030, 'parameter': 'CO2 intensity', 'value': 0.4, 'unit': 'tCO2/MWh', 'source': 'pypsa', 'further description': ''},
            {'technology': 'nuclear', 'year': 2030, 'parameter': 'CO2 intensity', 'value': 0.0, 'unit': 'tCO2/MWh', 'source': 'pypsa', 'further description': ''},
            # Efficiency
            {'technology': 'onwind', 'year': 2030, 'parameter': 'efficiency', 'value': 1.0, 'unit': '', 'source': 'pypsa', 'further description': ''},
            {'technology': 'offwind', 'year': 2030, 'parameter': 'efficiency', 'value': 1.0, 'unit': '', 'source': 'pypsa', 'further description': ''},
            {'technology': 'solar', 'year': 2030, 'parameter': 'efficiency', 'value': 1.0, 'unit': '', 'source': 'pypsa', 'further description': ''},
            {'technology': 'OCGT', 'year': 2030, 'parameter': 'efficiency', 'value': 0.4, 'unit': '', 'source': 'pypsa', 'further description': ''},
            {'technology': 'nuclear', 'year': 2030, 'parameter': 'efficiency', 'value': 0.33, 'unit': '', 'source': 'pypsa', 'further description': ''},
        ])
        
        # 4. Run the technoeconomic_data ETL function
        techno_etl = ETL_REGISTRY['technoeconomic_data']
        
        result = techno_etl(
            frames=remind_frames,
            mappings=mapping_data,
            pypsa_costs=pypsa_costs,
            currency_conversion=1.11,
            years=[2030]
        )
        
        # 5. Verify the results
        assert isinstance(result, pd.DataFrame)
        expected_columns = ['technology', 'year', 'parameter', 'value',
                            'unit', 'source', 'further description']
        assert all(col in result.columns for col in expected_columns)
        
        # Check that all PyPSA technologies are in result
        result_techs = set(result['technology'].unique())
        expected_techs = {'onwind', 'offwind', 'solar', 'OCGT', 'nuclear'}
        assert expected_techs.issubset(result_techs)
        
        # Verify specific mapping behaviors:
        # 1. use_remind: onwind should get REMIND windon investment cost (converted)
        onwind_investment = result.query(
            "technology == 'onwind' and parameter == 'investment'"
        )['value'].iloc[0]
        expected_onwind = 1.2 * 1e6 * 1.11  # TUSD/TW -> USD/MW * currency conversion
        assert abs(onwind_investment - expected_onwind) < 1e-3
        
        # 2. set_value: nuclear should get fixed 5000 USD/MW
        nuclear_investment = result[
            (result['technology'] == 'nuclear') &
            (result['parameter'] == 'investment')
        ]['value'].iloc[0]
        assert nuclear_investment == 5000 # set_value overrides
        
        # 3. weigh_remind_by_gen: OCGT should get weighted average of gaschp/gascc
        ocgt_investment = result[
            (result['technology'] == 'OCGT') &
            (result['parameter'] == 'investment')
        ]['value'].iloc[0]
        # Should be weighted: (0.6 * 0.3 + 0.9 * 0.7) * 1e6 * 1.11
        expected_ocgt = (0.6 * 0.3 + 0.9 * 0.7) * 1e6 * 1.11
        assert abs(ocgt_investment - expected_ocgt) < 1e-3
        
        # Verify that other parameters are also included (VOM, FOM, etc.)
        result_params = set(result['parameter'].unique())
        expected_params = {'investment', 'VOM', 'FOM', 'lifetime', 'CO2 intensity', 'efficiency'}
        assert expected_params.issubset(result_params)
        
        # Check unit conversions are applied
        onwind_vom = result[
            (result['technology'] == 'onwind') &
            (result['parameter'] == 'VOM')
        ]['value'].iloc[0]
        # Should be converted from TUSD/TWa to USD/MWh
        expected_vom = 0.01 * 1e6 / 8760 * 1.11
        assert abs(onwind_vom - expected_vom) < 1e-3

    def test_file_io_workflow(self, tmp_path):
        """Test file I/O operations in workflow."""
        # 1. Create sample data file
        sample_data = pd.DataFrame({
            'ttot': [2030, 2035],
            'all_regi': ['CHA', 'CHA'],
            'all_te': ['wind', 'solar'],
            'value': [100.0, 150.0]
        })
        
        file_path = tmp_path / "remind_data.csv"
        sample_data.to_csv(file_path, index=False)
        
        # 2. Read using REMIND CSV reader
        from rpycpl.utils import read_remind_csv
        result = read_remind_csv(file_path)
        
        # 3. Verify column mapping
        assert 'year' in result.columns
        assert 'region' in result.columns
        assert 'technology' in result.columns
        assert result['year'].tolist() == [2030, 2035]


class TestErrorHandling:
    """Test error handling in various scenarios."""

    def test_missing_etl_method(self):
        """Test error when ETL method doesn't exist."""
        with pytest.raises(KeyError):
            ETL_REGISTRY['nonexistent_method']

    def test_invalid_transformation_config(self):
        """Test handling of invalid transformation configuration."""
        # Missing required name field
        with pytest.raises(TypeError):
            Transformation(method="convert_load")  # Missing name

    def test_capacity_scaling_edge_cases(self):
        """Test edge cases in capacity scaling."""
        # Empty data
        empty_caps = pd.DataFrame(columns=['Tech', 'Fueltype', 'Capacity', 'tech_group'])
        empty_ref = pd.DataFrame(columns=['technology', 'capacity', 'tech_group', 'year'])
        
        # Should handle empty data gracefully
        result = scale_down_capacities(empty_caps, empty_ref)
        assert len(result) == 0

    def test_malformed_data_handling(self):
        """Test handling of malformed input data."""
        # Data with missing required columns
        malformed_data = pd.DataFrame({
            'year': [2030],
            # Missing 'value' column
            'technology': ['wind']
        })
        
        # Should handle gracefully or raise appropriate error
        try:
            from rpycpl.etl import convert_remind_capacities
            frames = {'capacities': malformed_data}
            convert_remind_capacities(frames)
        except (KeyError, AttributeError):
            # Expected behavior for malformed data
            pass


class TestScaling:
    """Test performance and scaling characteristics."""

    def test_dataset_handling(self):
        """Test handling of larger datasets."""
        # Create larger dataset
        years = list(range(2025, 2051))  # 26 years
        regions = ['USA', 'EUR', 'CHA', 'IND', 'BRA']  # 5 regions
        technologies = ['wind', 'solar', 'nuclear', 'gas', 'coal']  # 5 techs
        
        large_data = []
        for year in years:
            for region in regions:
                for tech in technologies:
                    large_data.append({
                        'year': year,
                        'region': region,
                        'technology': tech,
                        'value': year * 0.1  # Dummy value
                    })
        
        large_df = pd.DataFrame(large_data)
        
        # Test capacity conversion with large dataset
        from rpycpl.etl import convert_remind_capacities
        frames = {'capacities': large_df}
        
        result = convert_remind_capacities(frames, region='CHA')
        
        # Should handle large dataset efficiently
        assert len(result) == len(years) * len(technologies)


if __name__ == "__main__":
    # Run integration tests
    pytest.main([__file__, "-v"])
