"""Test imports and basic package structure."""


def test_import_main_package():
    """Test that the main package imports correctly."""
    import rpycpl
    assert rpycpl is not None


def test_import_utils():
    """Test that utils module imports correctly."""
    from rpycpl import utils
    assert utils is not None


def test_import_etl():
    """Test that etl module imports correctly."""
    from rpycpl import etl
    assert etl is not None


def test_import_capacities_etl():
    """Test that capacities_etl module imports correctly."""
    from rpycpl import capacities_etl
    assert capacities_etl is not None


def test_import_technoecon_etl():
    """Test that technoecon_etl module imports correctly."""
    from rpycpl import technoecon_etl
    assert technoecon_etl is not None


def test_import_disagg():
    """Test that disagg module imports correctly."""
    from rpycpl import disagg
    assert disagg is not None


def test_import_coupled_cfg():
    """Test that coupled_cfg module imports correctly."""
    from rpycpl import coupled_cfg
    assert coupled_cfg is not None


def test_etl_registry_exists():
    """Test that the ETL registry is accessible."""
    from rpycpl.etl import ETL_REGISTRY
    assert isinstance(ETL_REGISTRY, dict)


def test_readers_registry_exists():
    """Test that the readers registry is accessible."""
    from rpycpl.utils import READERS_REGISTRY
    assert isinstance(READERS_REGISTRY, dict)
