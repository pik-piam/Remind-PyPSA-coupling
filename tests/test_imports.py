"""Test imports and basic package structure."""


def test_import_main_package():
    """The main package imports and exposes the public API."""
    import iampypsa

    assert iampypsa is not None
    for name in ("CouplingAdapter", "RemindLoader", "load_symbol_specs"):
        assert hasattr(iampypsa, name), name


def test_import_subpackages():
    """The current sub-packages import correctly."""
    from iampypsa import adapters, downscale, io, transforms  # noqa: F401
    from iampypsa import units, validate  # noqa: F401
    from iampypsa.io import remind_symbols  # noqa: F401


def test_loader_importable():
    """The unified loader + symbol layer are importable from io."""
    from iampypsa.io import RemindLoader, load_frame, load_symbol_specs  # noqa: F401

    assert RemindLoader is not None
