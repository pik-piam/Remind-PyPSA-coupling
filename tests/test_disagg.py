"""Tests for rpycpl.disagg module."""
import pandas as pd
import pytest

from rpycpl.disagg import SpatialDisaggregator


class TestSpatialDisaggregator:
    """Test cases for SpatialDisaggregator class."""

    def test_init_without_targets(self):
        """Test initialization without target nodes."""
        disagg = SpatialDisaggregator()
        assert disagg._target_nodes is None

    def test_init_with_targets(self):
        """Test initialization with target nodes."""
        targets = ['node1', 'node2', 'node3']
        disagg = SpatialDisaggregator(targets=targets)
        assert disagg._target_nodes == targets

    def test_validate_reference_data_valid(self):
        """Test validation of valid reference data."""
        disagg = SpatialDisaggregator()
        ref_data = pd.Series([0.3, 0.4, 0.3], index=['node1', 'node2', 'node3'])
        
        # Should not raise any exception
        disagg.validate_reference_data(ref_data)

    def test_validate_reference_data_not_series(self):
        """Test validation fails for non-Series data."""
        disagg = SpatialDisaggregator()
        ref_data = [0.3, 0.4, 0.3]  # List instead of Series
        
        with pytest.raises(TypeError, match="must be a pandas Series"):
            disagg.validate_reference_data(ref_data)

    def test_validate_reference_data_not_normalized(self):
        """Test validation fails for non-normalized data."""
        disagg = SpatialDisaggregator()
        ref_data = pd.Series([0.3, 0.4, 0.5], index=['node1', 'node2', 'node3'])  # Sum = 1.2
        
        with pytest.raises(ValueError, match="not normalised to 1"):
            disagg.validate_reference_data(ref_data)

    def test_validate_reference_data_wrong_nodes(self):
        """Test validation fails for wrong target nodes."""
        targets = ['node1', 'node2']
        disagg = SpatialDisaggregator(targets=targets)
        ref_data = pd.Series([0.5, 0.5], index=['node3', 'node4'])  # Wrong nodes
        
        with pytest.raises(ValueError, match="does not match target nodes"):
            disagg.validate_reference_data(ref_data)

    def test_validate_reference_data_correct_nodes(self):
        """Test validation passes for correct target nodes."""
        targets = ['node1', 'node2']
        disagg = SpatialDisaggregator(targets=targets)
        ref_data = pd.Series([0.6, 0.4], index=['node1', 'node2'])
        
        # Should not raise any exception
        disagg.validate_reference_data(ref_data)

    def test_use_static_reference_basic(self):
        """Test basic static reference disaggregation."""
        disagg = SpatialDisaggregator()
        
        # Time series data (by year)
        data = pd.Series([1000, 1200, 1400], index=[2030, 2035, 2040])
        
        # Reference spatial distribution
        ref_data = pd.Series([0.4, 0.3, 0.3], index=['node1', 'node2', 'node3'])
        
        result = disagg.use_static_reference(data, ref_data)
        
        # Check structure
        assert isinstance(result, pd.DataFrame)
        assert result.shape == (3, 3)  # 3 nodes x 3 years
        
        # Check values - each year should be distributed according to reference
        assert abs(result.loc['node1', 2030] - 1000 * 0.4) < 1e-6  # 400
        assert abs(result.loc['node2', 2030] - 1000 * 0.3) < 1e-6  # 300
        assert abs(result.loc['node3', 2030] - 1000 * 0.3) < 1e-6  # 300
        
        assert abs(result.loc['node1', 2035] - 1200 * 0.4) < 1e-6  # 480
        assert abs(result.loc['node2', 2035] - 1200 * 0.3) < 1e-6  # 360

    def test_use_static_reference_single_year(self):
        """Test disaggregation with single year data."""
        disagg = SpatialDisaggregator()
        
        data = pd.Series([500], index=[2030])
        ref_data = pd.Series([0.7, 0.3], index=['node1', 'node2'])
        
        result = disagg.use_static_reference(data, ref_data)
        
        assert result.shape == (2, 1)  # 2 nodes x 1 year
        assert abs(result.loc['node1', 2030] - 500 * 0.7) < 1e-6  # 350
        assert abs(result.loc['node2', 2030] - 500 * 0.3) < 1e-6  # 150

    def test_use_static_reference_single_node(self):
        """Test disaggregation with single node."""
        disagg = SpatialDisaggregator()
        
        data = pd.Series([1000, 1500], index=[2030, 2035])
        ref_data = pd.Series([1.0], index=['node1'])  # All to one node
        
        result = disagg.use_static_reference(data, ref_data)
        
        assert result.shape == (1, 2)  # 1 node x 2 years
        assert result.loc['node1', 2030] == 1000
        assert result.loc['node1', 2035] == 1500

    def test_use_static_reference_validation_called(self):
        """Test that validation is called in use_static_reference."""
        disagg = SpatialDisaggregator()
        
        data = pd.Series([1000], index=[2030])
        invalid_ref_data = pd.Series([0.5, 0.7], index=['node1', 'node2'])  # Sum > 1
        
        with pytest.raises(ValueError, match="not normalised to 1"):
            disagg.use_static_reference(data, invalid_ref_data)

    def test_use_static_reference_empty_data(self):
        """Test disaggregation with empty data."""
        disagg = SpatialDisaggregator()
        
        data = pd.Series([], dtype=float)
        ref_data = pd.Series([0.5, 0.5], index=['node1', 'node2'])
        
        result = disagg.use_static_reference(data, ref_data)
        
        # Should handle empty data gracefully
        assert result.shape[0] == 2  # Still has nodes
        assert result.shape[1] == 0  # But no years

    def test_use_static_reference_zero_values(self):
        """Test disaggregation with zero values in data."""
        disagg = SpatialDisaggregator()
        
        data = pd.Series([0, 1000, 0], index=[2030, 2035, 2040])
        ref_data = pd.Series([0.6, 0.4], index=['node1', 'node2'])
        
        result = disagg.use_static_reference(data, ref_data)
        
        # Zero values should remain zero after distribution
        assert result.loc['node1', 2030] == 0
        assert result.loc['node2', 2030] == 0
        assert abs(result.loc['node1', 2035] - 600) < 1e-6
        assert abs(result.loc['node2', 2035] - 400) < 1e-6
        assert result.loc['node1', 2040] == 0
        assert result.loc['node2', 2040] == 0
